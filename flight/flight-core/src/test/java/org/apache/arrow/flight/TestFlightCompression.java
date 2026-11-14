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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that Flight writers compress IPC bodies as requested by {@link IpcOption}. */
public class TestFlightCompression {

  private static final byte[] COMMAND = "data".getBytes(StandardCharsets.UTF_8);
  private static final int ROWS = 10_000;
  private static final int BATCHES = 2;
  private static final String[] DICTIONARY_VALUES = {"alpha", "beta", "gamma", "delta", "epsilon"};
  private static final DictionaryEncoding ENCODING =
      new DictionaryEncoding(1L, /* ordered */ false, new ArrowType.Int(32, true));
  private static final Schema SCHEMA =
      new Schema(
          Arrays.asList(
              Field.notNullable("i", new ArrowType.Int(32, true)),
              new Field(
                  "encoded",
                  new FieldType(true, new ArrowType.Int(32, true), ENCODING),
                  Collections.emptyList())));

  private BufferAllocator allocator;
  private Producer producer;
  private FlightServer server;
  private FlightClient client;

  @BeforeEach
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    producer = new Producer(allocator);
    server =
        FlightServer.builder(
                allocator, Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0), producer)
            .build()
            .start();
    client = newClient(server.getPort());
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Closing the allocator fails the test if any (compressed) buffer was leaked.
    AutoCloseables.close(client, server, allocator);
  }

  static Stream<IpcOption> options() {
    return Stream.of(
        IpcOption.DEFAULT,
        IpcOption.DEFAULT.withCompression(CodecType.ZSTD),
        IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 7),
        IpcOption.DEFAULT.withCompression(CodecType.LZ4_FRAME));
  }

  @ParameterizedTest
  @MethodSource("options")
  public void testDoGet(IpcOption option) throws Exception {
    producer.option = option;
    try (final FlightStream stream = client.getStream(new Ticket(COMMAND))) {
      verifyStream(stream);
    }
  }

  /** As {@link #testDoGet}, with the zero-copy write path, which retains the compressed buffers. */
  @ParameterizedTest
  @MethodSource("options")
  public void testDoGetZeroCopy(IpcOption option) throws Exception {
    producer.option = option;
    producer.zeroCopy = true;
    try (final FlightStream stream = client.getStream(new Ticket(COMMAND))) {
      verifyStream(stream);
    }
  }

  @ParameterizedTest
  @MethodSource("options")
  public void testDoPut(IpcOption option) throws Exception {
    putStream(client, option);
  }

  @ParameterizedTest
  @MethodSource("options")
  public void testDoExchange(IpcOption option) throws Exception {
    producer.option = option;
    try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator);
        final FlightClient.ExchangeReaderWriter exchange =
            client.doExchange(FlightDescriptor.command(COMMAND))) {
      // Both sides compress: the server echoes the client's data.
      writeStream(exchange.getWriter(), root, provider, option);
      verifyStream(exchange.getReader());
    }
  }

  /**
   * The in-process transport hands the serialized message to the reader without copying it, so it
   * is read after the writer has closed the message.
   */
  @ParameterizedTest
  @MethodSource("options")
  public void testInProcess(IpcOption option) throws Exception {
    producer.option = option;
    final String name = InProcessServerBuilder.generateName();
    final ExecutorService executor = Executors.newCachedThreadPool();
    final Server inProcessServer =
        InProcessServerBuilder.forName(name)
            .addService(
                FlightGrpcUtils.createFlightService(
                    allocator, producer, ServerAuthHandler.NO_OP, executor))
            .build()
            .start();
    try (final FlightClient inProcessClient =
        FlightGrpcUtils.createFlightClient(
            allocator, InProcessChannelBuilder.forName(name).build())) {
      try (final FlightStream stream = inProcessClient.getStream(new Ticket(COMMAND))) {
        verifyStream(stream);
      }
      putStream(inProcessClient, option);
    } finally {
      inProcessServer.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
  }

  /**
   * gRPC may serialize a message after the writer has closed it: on the in-process transport,
   * before a client call is connected, or again when a call is retried. Compressed messages must
   * stay serializable then, like uncompressed ones whose vectors are still alive.
   */
  @ParameterizedTest
  @MethodSource("options")
  public void testSerializeAfterClose(IpcOption option) throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (final VectorSchemaRoot root = createRoot(allocator);
        final VectorSchemaRoot received = VectorSchemaRoot.create(SCHEMA, allocator)) {
      final CompressionCodec codec =
          CompressionUtil.createCodec(
              CompressionCodec.Factory.INSTANCE, option.codecType, option.compressionLevel);
      final ArrowMessage message =
          new ArrowMessage(
              new VectorUnloader(root, /* includeNullCount */ true, codec, /* alignBuffers */ true)
                  .getRecordBatch(),
              /* appMetadata */ null,
              /* tryZeroCopy */ false,
              option);
      message.close();
      // Serialize twice, as a retry would.
      for (int attempt = 0; attempt < 2; attempt++) {
        try (final InputStream stream = marshaller.stream(message);
            final ArrowMessage parsed = marshaller.parse(stream);
            final ArrowRecordBatch batch = parsed.asRecordBatch()) {
          new VectorLoader(received).load(batch);
          final IntVector ints = (IntVector) received.getVector("i");
          for (int row = 0; row < ROWS; row++) {
            assertEquals(row % 16, ints.get(row));
          }
        }
      }
    }
  }

  /** A server writing DoGet compresses dictionary batches and record batches. */
  @ParameterizedTest
  @MethodSource("options")
  public void testServerWireFormat(IpcOption option) throws Exception {
    producer.option = option;
    final List<Flight.FlightData> messages = new ArrayList<>();
    final ManagedChannel channel =
        NettyChannelBuilder.forAddress(FlightTestUtil.LOCALHOST, server.getPort())
            .usePlaintext()
            .build();
    try {
      FlightServiceGrpc.newBlockingStub(channel)
          .doGet(Flight.Ticket.newBuilder().setTicket(ByteString.copyFrom(COMMAND)).build())
          .forEachRemaining(messages::add);
    } finally {
      channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    }
    verifyWireFormat(option, messages);
  }

  /** DoPut the root with the given option, which the producer verifies. */
  private void putStream(FlightClient client, IpcOption option) throws Exception {
    producer.verifiedPutBatches = 0;
    try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.ClientStreamListener listener =
          client.startPut(FlightDescriptor.command(COMMAND), new AsyncPutListener());
      writeStream(listener, root, provider, option);
      listener.getResult();
    }
    assertEquals(BATCHES, producer.verifiedPutBatches);
  }

  private FlightClient newClient(int port) {
    return FlightClient.builder(allocator, Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, port))
        .build();
  }

  private static DictionaryProvider.MapDictionaryProvider createProvider(
      BufferAllocator allocator) {
    final VarCharVector dictionary = new VarCharVector("dictionary", allocator);
    dictionary.allocateNew();
    for (int i = 0; i < DICTIONARY_VALUES.length; i++) {
      dictionary.setSafe(i, DICTIONARY_VALUES[i].getBytes(StandardCharsets.UTF_8));
    }
    dictionary.setValueCount(DICTIONARY_VALUES.length);
    return new DictionaryProvider.MapDictionaryProvider(new Dictionary(dictionary, ENCODING));
  }

  private static String expectedValue(int row) {
    return row % 11 == 0 ? null : DICTIONARY_VALUES[row % DICTIONARY_VALUES.length];
  }

  /** Create a root holding a highly compressible batch. */
  private static VectorSchemaRoot createRoot(BufferAllocator allocator) {
    final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
    final IntVector ints = (IntVector) root.getVector("i");
    final IntVector indices = (IntVector) root.getVector("encoded");
    ints.allocateNew(ROWS);
    indices.allocateNew(ROWS);
    for (int row = 0; row < ROWS; row++) {
      ints.set(row, row % 16);
      if (expectedValue(row) != null) {
        indices.set(row, row % DICTIONARY_VALUES.length);
      }
    }
    root.setRowCount(ROWS);
    return root;
  }

  /** Send the root {@link #BATCHES} times, then complete the stream. */
  private static void writeStream(
      OutboundStreamListener listener,
      VectorSchemaRoot root,
      DictionaryProvider provider,
      IpcOption option) {
    listener.start(root, provider, option);
    for (int batch = 0; batch < BATCHES; batch++) {
      listener.putNext();
    }
    listener.completed();
  }

  /** Read and verify the batches written by {@link #writeStream}. */
  private static void verifyStream(FlightStream stream) {
    int batches = 0;
    while (stream.next()) {
      final VectorSchemaRoot root = stream.getRoot();
      assertEquals(ROWS, root.getRowCount());
      final IntVector ints = (IntVector) root.getVector("i");
      final Dictionary dictionary = stream.getDictionaryProvider().lookup(ENCODING.getId());
      try (final ValueVector decoded =
          DictionaryEncoder.decode(root.getVector("encoded"), dictionary)) {
        for (int row = 0; row < ROWS; row++) {
          assertEquals(row % 16, ints.get(row));
          final Object value = decoded.getObject(row);
          if (!Objects.equals(expectedValue(row), value == null ? null : value.toString())) {
            throw new AssertionError("Row " + row + " differs: " + value);
          }
        }
      }
      batches++;
    }
    assertEquals(BATCHES, batches);
  }

  /**
   * Check the FlightData messages of a stream written by {@link #writeStream}, exactly as sent: a
   * schema, a dictionary batch, then record batches, with every batch compressed by the requested
   * codec.
   */
  private void verifyWireFormat(IpcOption option, List<Flight.FlightData> messages)
      throws Exception {
    final List<Flight.FlightData> data = new ArrayList<>();
    for (Flight.FlightData message : messages) {
      if (!message.getDataHeader().isEmpty()) {
        data.add(message);
      }
    }
    assertEquals(2 + BATCHES, data.size());
    assertEquals(MessageHeader.Schema, header(data.get(0)).headerType());
    assertEquals(MessageHeader.DictionaryBatch, header(data.get(1)).headerType());
    for (Flight.FlightData message : data.subList(1, data.size())) {
      try (final ArrowRecordBatch batch = deserializeBatch(message)) {
        assertEquals(
            option.codecType, CodecType.fromCompressionType(batch.getBodyCompression().getCodec()));
        if (header(message).headerType() == MessageHeader.RecordBatch
            && option.codecType != CodecType.NO_COMPRESSION) {
          // Buffers are i (validity, data), encoded (validity, data). A compressed buffer is
          // prefixed by its uncompressed length, or -1 if it was left uncompressed.
          final ArrowBuf intData = batch.getBuffers().get(1);
          assertEquals((long) ROWS * Integer.BYTES, intData.getLong(0));
          assertTrue(intData.readableBytes() < ROWS * Integer.BYTES / 4);
        }
      }
    }
  }

  private static Message header(Flight.FlightData data) {
    return Message.getRootAsMessage(data.getDataHeader().asReadOnlyByteBuffer());
  }

  /** Deserialize a record batch or dictionary batch without decompressing its buffers. */
  private ArrowRecordBatch deserializeBatch(Flight.FlightData data) throws Exception {
    final Message message = header(data);
    final ByteBuffer bodyBytes =
        data.getDataBody().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    // The deserialized batch takes ownership of the body.
    final ArrowBuf body = allocator.buffer(bodyBytes.remaining());
    try {
      body.setBytes(0, bodyBytes);
      body.writerIndex(data.getDataBody().size());
      return message.headerType() == MessageHeader.RecordBatch
          ? MessageSerializer.deserializeRecordBatch(message, body)
          : MessageSerializer.deserializeDictionaryBatch(message, body).getDictionary();
    } catch (Exception e) {
      body.close();
      throw e;
    }
  }

  /** Writes with the configured option; verifies DoPut data; echoes DoExchange data. */
  static class Producer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    volatile IpcOption option = IpcOption.DEFAULT;
    volatile boolean zeroCopy = false;
    volatile int verifiedPutBatches = 0;

    Producer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
          final VectorSchemaRoot root = createRoot(allocator)) {
        listener.setUseZeroCopy(zeroCopy);
        writeStream(listener, root, provider, option);
      }
    }

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        try {
          verifyStream(flightStream);
          verifiedPutBatches = BATCHES;
          ackStream.onCompleted();
        } catch (Throwable t) {
          ackStream.onError(
              CallStatus.INVALID_ARGUMENT.withDescription(t.toString()).toRuntimeException());
        }
      };
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      // Read the first batch before starting, so that its dictionary has been received.
      if (reader.next()) {
        writer.start(reader.getRoot(), reader.getDictionaryProvider(), option);
        do {
          writer.putNext();
        } while (reader.next());
      }
      writer.completed();
    }
  }
}
