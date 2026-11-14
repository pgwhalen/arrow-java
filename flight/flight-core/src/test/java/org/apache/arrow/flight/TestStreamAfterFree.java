/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * gRPC may read a serialized message after the writer has closed it and refilled its root, e.g. on
 * the in-process transport, which hands the serialized stream to the reader as is. Without
 * zero-copy, the stream wraps the vectors' memory but holds no reference to it, so it is read after
 * it was freed, and possibly reused, without any error.
 */
public class TestStreamAfterFree {
  private static final Schema SCHEMA =
      new Schema(Collections.singletonList(Field.notNullable("i", new ArrowType.Int(32, true))));
  private static final int ROWS = 1024;
  private static final int BATCHES = 4;

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  /** The writer refills the root between serializing a message and gRPC reading the stream. */
  @Test
  public void testStreamReadAfterRootRefilled() throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      fill(root, 1);
      final ArrowBuf data = ((IntVector) root.getVector("i")).getDataBuffer();
      final InputStream stream;
      try (final ArrowMessage message =
          new ArrowMessage(
              new VectorUnloader(root).getRecordBatch(),
              /* appMetadata */ null,
              /* tryZeroCopy */ false,
              IpcOption.DEFAULT)) {
        stream = marshaller.stream(message);
      }
      // The writer moves on to the next batch.
      root.clear();
      fill(root, 2);

      try (final InputStream s = stream;
          final ArrowMessage parsed = marshaller.parse(s)) {
        // Reading the freed memory raises no error, but returns the next batch's data.
        assertEquals(1, firstValue(parsed));
        assertTrue(data.refCnt() > 0, "The stream's body was freed before gRPC read it");
      }
    }
  }

  /**
   * End to end over the in-process transport: a DoGet producer that allocates each batch anew sends
   * every batch, and closes its root, before the client gets to read them.
   */
  @Test
  public void testInProcessDoGet() throws Exception {
    final String name = InProcessServerBuilder.generateName();
    final ExecutorService serverExecutor = Executors.newCachedThreadPool();
    final GatedExecutor clientExecutor = new GatedExecutor();
    final Server server =
        InProcessServerBuilder.forName(name)
            .addService(
                FlightGrpcUtils.createFlightService(
                    allocator, new Producer(allocator), ServerAuthHandler.NO_OP, serverExecutor))
            .build()
            .start();
    final ManagedChannel channel =
        InProcessChannelBuilder.forName(name).executor(clientExecutor).build();
    try (final FlightClient client = FlightGrpcUtils.createFlightClient(allocator, channel)) {
      final FlightStream stream = client.getStream(new Ticket(new byte[0]));
      try {
        // Until the client reads them, gRPC holds every batch's serialized stream.
        Producer.done.await();
        clientExecutor.open();
        final List<Integer> received = new ArrayList<>();
        while (stream.next()) {
          received.add(((IntVector) stream.getRoot().getVector("i")).get(0));
        }
        assertEquals(List.of(0, 1, 2, 3), received);
      } finally {
        clientExecutor.open();
        stream.close();
      }
    } finally {
      AutoCloseables.close(
          () -> server.shutdownNow().awaitTermination(30, TimeUnit.SECONDS),
          serverExecutor::shutdownNow,
          channel::shutdownNow);
    }
  }

  private static void fill(VectorSchemaRoot root, int value) {
    final IntVector ints = (IntVector) root.getVector("i");
    ints.allocateNew(ROWS);
    for (int row = 0; row < ROWS; row++) {
      ints.set(row, value);
    }
    root.setRowCount(ROWS);
  }

  private int firstValue(ArrowMessage parsed) throws Exception {
    try (final VectorSchemaRoot received = VectorSchemaRoot.create(SCHEMA, allocator);
        final ArrowRecordBatch batch = parsed.asRecordBatch()) {
      new VectorLoader(received).load(batch);
      return ((IntVector) received.getVector("i")).get(0);
    }
  }

  /** Writes batches 0, 1, ... with a fresh allocation for each, like a typical producer. */
  static final class Producer extends NoOpFlightProducer {
    static final java.util.concurrent.CountDownLatch done =
        new java.util.concurrent.CountDownLatch(1);
    private final BufferAllocator allocator;

    Producer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        listener.start(root);
        for (int batch = 0; batch < BATCHES; batch++) {
          root.clear();
          fill(root, batch);
          listener.putNext();
        }
        listener.completed();
      } finally {
        done.countDown();
      }
    }
  }

  /** Runs nothing until opened, so the client reads nothing until then. */
  private static final class GatedExecutor implements Executor {
    private final List<Runnable> queued = new ArrayList<>();
    private boolean open = false;

    @Override
    public void execute(Runnable command) {
      synchronized (this) {
        if (!open) {
          queued.add(command);
          return;
        }
      }
      command.run();
    }

    void open() {
      final List<Runnable> toRun;
      synchronized (this) {
        if (open) {
          return;
        }
        open = true;
        toRun = new ArrayList<>(queued);
        queued.clear();
      }
      toRun.forEach(Runnable::run);
    }
  }
}
