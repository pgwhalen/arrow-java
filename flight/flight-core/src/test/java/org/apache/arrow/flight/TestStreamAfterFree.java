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
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
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
 * A Flight writer closes each message, and the caller refills or frees its vectors, as soon as
 * {@code onNext} returns. gRPC's marshaller contract allows it to use the message later than that:
 *
 * <ol>
 *   <li>It may read the serialized stream later, e.g. on the in-process transport, which hands the
 *       stream to the reader as is. Without zero-copy, the stream wraps the vectors' memory but
 *       holds no reference to it, so it is read after it was freed, and possibly reused, without
 *       any error.
 *   <li>A client call may serialize the message object again: gRPC keeps every message sent before
 *       the call commits and replays them all when it retries the call. Serializing a message a
 *       second time fails, because the first serialization consumed its IPC header, and by then the
 *       body may have been freed as well.
 * </ol>
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

  /**
   * Serializing a message must be repeatable, because a retry serializes it again. The first
   * serialization consumes the IPC header buffer, so the second one silently sends an empty header.
   */
  @Test
  public void testMessageSerializedTwice() throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        final ArrowMessage message = recordBatchMessage(root, 1)) {
      for (int attempt = 0; attempt < 2; attempt++) {
        try (final InputStream stream = marshaller.stream(message);
            final ArrowMessage parsed = marshaller.parse(stream)) {
          assertEquals(
              ArrowMessage.HeaderType.RECORD_BATCH,
              parsed.getMessageType(),
              "serialization " + (attempt + 1));
          assertEquals(1, firstValue(parsed));
        }
      }
    }
  }

  /**
   * A retry serializes a message again after the writer closed it and refilled its root. The
   * replayed message should carry the original data.
   */
  @Test
  public void testMessageSerializedAgainAfterRootRefilled() throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      final ArrowMessage message = recordBatchMessage(root, 1);
      try (final InputStream first = marshaller.stream(message);
          final ArrowMessage parsed = marshaller.parse(first)) {
        assertEquals(1, firstValue(parsed));
      }
      // The writer closes the message and moves on to the next batch.
      message.close();
      root.clear();
      fill(root, 2);

      // gRPC retries the call and serializes the message again.
      try (final InputStream again = marshaller.stream(message);
          final ArrowMessage parsed = marshaller.parse(again)) {
        assertEquals(1, firstValue(parsed));
      }
    }
  }

  /**
   * End to end over Netty: the server refuses the first attempt of a DoPut after the client has
   * sent everything, so gRPC retries the call and replays the buffered messages. The server should
   * receive every batch on the second attempt.
   *
   * <p>This uses a retry policy so the retry is deterministic; gRPC replays messages the same way
   * on a transparent retry (e.g. when a server refuses a stream), which needs no configuration.
   */
  @Test
  public void testDoPutRetried() throws Exception {
    final ExecutorService serverExecutor = Executors.newCachedThreadPool();
    final PutProducer producer = new PutProducer();
    final RefuseFirstAttempt refuseFirstAttempt = new RefuseFirstAttempt();
    final Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
            .addService(
                ServerInterceptors.intercept(
                    FlightGrpcUtils.createFlightService(
                        allocator, producer, ServerAuthHandler.NO_OP, serverExecutor),
                    refuseFirstAttempt))
            .build()
            .start();
    final ManagedChannel channel =
        NettyChannelBuilder.forAddress("localhost", server.getPort())
            .usePlaintext()
            .defaultServiceConfig(retryDoPutOnce())
            .enableRetry()
            .build();
    try (final FlightClient client = FlightGrpcUtils.createFlightClient(allocator, channel);
        final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      final FlightClient.ClientStreamListener listener =
          client.startPut(
              FlightDescriptor.path("retried"),
              root,
              new AsyncPutListener(),
              CallOptions.timeout(10, TimeUnit.SECONDS));
      for (int batch = 0; batch < BATCHES; batch++) {
        root.clear();
        fill(root, batch);
        listener.putNext();
      }
      listener.completed();
      listener.getResult();
      assertEquals(2, refuseFirstAttempt.attempts.get(), "the call was not retried");
      assertEquals(List.of(0, 1, 2, 3), producer.received);
    } finally {
      AutoCloseables.close(
          () -> channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS),
          () -> server.shutdownNow().awaitTermination(30, TimeUnit.SECONDS),
          serverExecutor::shutdownNow);
    }
  }

  /** A service config that retries DoPut once on UNAVAILABLE. */
  private static Map<String, Object> retryDoPutOnce() {
    final Map<String, Object> retryPolicy =
        Map.of(
            "maxAttempts",
            2.0,
            "initialBackoff",
            "0.01s",
            "maxBackoff",
            "0.01s",
            "backoffMultiplier",
            1.0,
            "retryableStatusCodes",
            List.of("UNAVAILABLE"));
    final Map<String, Object> name =
        Map.of("service", FlightServiceGrpc.SERVICE_NAME, "method", "DoPut");
    return Map.of(
        "methodConfig", List.of(Map.of("name", List.of(name), "retryPolicy", retryPolicy)));
  }

  /**
   * Fails the first DoPut with UNAVAILABLE once the client has sent everything, without sending
   * headers, so the client call is not committed and may be retried.
   */
  private static final class RefuseFirstAttempt implements ServerInterceptor {
    final AtomicInteger attempts = new AtomicInteger();

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      if (!call.getMethodDescriptor().getBareMethodName().equals("DoPut")
          || attempts.incrementAndGet() > 1) {
        return next.startCall(call, headers);
      }
      call.request(Integer.MAX_VALUE);
      return new ServerCall.Listener<ReqT>() {
        @Override
        public void onMessage(ReqT message) {
          try {
            ((AutoCloseable) message).close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public void onHalfClose() {
          call.close(Status.UNAVAILABLE.withDescription("refused first attempt"), new Metadata());
        }
      };
    }
  }

  /** Records the first value of each batch a DoPut receives. */
  static final class PutProducer extends NoOpFlightProducer {
    final List<Integer> received = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        while (flightStream.next()) {
          received.add(((IntVector) flightStream.getRoot().getVector("i")).get(0));
        }
        ackStream.onCompleted();
      };
    }
  }

  /** Fill the root with {@code value} and create a (non-zero-copy) message for it. */
  private static ArrowMessage recordBatchMessage(VectorSchemaRoot root, int value) {
    fill(root, value);
    return new ArrowMessage(
        new VectorUnloader(root).getRecordBatch(),
        /* appMetadata */ null,
        /* tryZeroCopy */ false,
        IpcOption.DEFAULT);
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
