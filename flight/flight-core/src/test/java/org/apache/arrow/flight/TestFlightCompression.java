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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.junit.jupiter.params.provider.Arguments;
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

  /**
   * The compressing options. Tests that read a stream after the writer freed an uncompressed body
   * (without zero-copy) use only these: that is an existing bug, unrelated to compression, which
   * apache/arrow-java PR #2 (TestStreamAfterFree) reproduces.
   */
  static Stream<IpcOption> compressedOptions() {
    return options().filter(option -> option.codecType != CodecType.NO_COMPRESSION);
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
    putStream(client, option, /* zeroCopy */ false);
  }

  /** As {@link #testDoPut}, with the zero-copy write path. */
  @ParameterizedTest
  @MethodSource("options")
  public void testDoPutZeroCopy(IpcOption option) throws Exception {
    putStream(client, option, /* zeroCopy */ true);
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
   * is read after the writer has closed the message (see {@link #compressedOptions()}).
   */
  @ParameterizedTest
  @MethodSource("compressedOptions")
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
      putStream(inProcessClient, option, /* zeroCopy */ false);
    } finally {
      inProcessServer.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
  }

  static Stream<Arguments> optionsAndZeroCopy() {
    return options()
        .flatMap(option -> Stream.of(Arguments.of(option, false), Arguments.of(option, true)));
  }

  /** Every combination except an uncompressed body without zero-copy (see compressedOptions()). */
  static Stream<Arguments> optionsAndZeroCopyRetainingBody() {
    return optionsAndZeroCopy()
        .filter(
            args ->
                ((IpcOption) args.get()[0]).codecType != CodecType.NO_COMPRESSION
                    || (Boolean) args.get()[1]);
  }

  /**
   * gRPC may read a serialized message after the writer has closed it and moved on, e.g. on the
   * in-process transport. A compressed body is owned by the message alone, so the stream must keep
   * it alive, as it does for zero-copy writes.
   */
  @ParameterizedTest
  @MethodSource("optionsAndZeroCopyRetainingBody")
  public void testStreamOutlivesMessage(IpcOption option, boolean zeroCopy) throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    final InputStream stream;
    try (final VectorSchemaRoot root = createRoot(allocator)) {
      final ArrowBuf appMetadata = allocator.buffer(Long.BYTES);
      appMetadata.writeLong(42);
      try (final ArrowMessage message =
          new ArrowMessage(recordBatch(root, option), appMetadata, zeroCopy, option)) {
        stream = marshaller.stream(message);
      }
      // As a writer would before refilling it.
      root.clear();
    }
    try (final InputStream s = stream;
        final ArrowMessage parsed = marshaller.parse(s)) {
      assertEquals(42, parsed.getApplicationMetadata().getLong(0));
      verifyBatch(parsed);
    }
  }

  /**
   * Goal: compressed bodies live as long as uncompressed ones.
   *
   * <p>A client call may serialize a message object after the writer closed it, e.g. it queues
   * messages until it is connected. Whether the body is compressed must not change what can still
   * be serialized: the dictionary for the whole call, a record batch until the writer refills the
   * root.
   */
  @ParameterizedTest
  @MethodSource("options")
  public void testClientBodyLifetime(IpcOption option) throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    final MessageQueueingObserver observer = new MessageQueueingObserver();
    final CompressedBodyHolder compressedBodies = new CompressedBodyHolder();
    try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.PutObserver writer =
          new FlightClient.PutObserver(
              FlightDescriptor.command(COMMAND),
              observer,
              compressedBodies,
              /* isCancelled */ () -> false,
              /* getResult */ () -> {});
      // Not yet connected: the call queues messages.
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> writer.start(root, provider, option));
      observer.ready = true;
      writer.putNext();
      // The writer refills the root, freeing the first batch's uncompressed body, and sends again.
      root.clear();
      fillRoot(root);
      writer.putNext();

      // The call serializes the queued messages, all closed by now: the dictionary and the latest
      // batch are still alive, whether or not they are compressed.
      final List<ArrowMessage> sent = observer.messages;
      assertEquals(4, sent.size());
      verifySerialized(marshaller, sent.get(0));
      verifySerialized(marshaller, sent.get(1));
      verifySerialized(marshaller, sent.get(3));

      writer.completed();
      compressedBodies.callEnded();
    }
    // tearDown fails the test if the call's end did not release the compressed bodies.
  }

  /**
   * Goal: start() does not wait for the call. A client call that never becomes ready, with a
   * listener that never reports it cancelled, must not block start() (as startPut() calls it).
   */
  @ParameterizedTest
  @MethodSource("options")
  public void testStartDoesNotWait(IpcOption option) throws Exception {
    final MessageQueueingObserver observer = new MessageQueueingObserver();
    final CompressedBodyHolder compressedBodies = new CompressedBodyHolder();
    try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.PutObserver writer =
          new FlightClient.PutObserver(
              FlightDescriptor.command(COMMAND),
              observer,
              compressedBodies,
              /* isCancelled */ () -> false,
              /* getResult */ () -> {});
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> writer.start(root, provider, option));
      // The schema and dictionary are queued for when the call connects.
      assertEquals(2, observer.messages.size());
      compressedBodies.callEnded();
    }
  }

  /**
   * Goal: start() does not wait for the call, end to end. The call fails before it ever connects;
   * startPut() returns, the failure is reported by getResult(), and the compressed dictionary held
   * for the queued message is released (checked by tearDown).
   */
  @ParameterizedTest
  @MethodSource("options")
  public void testStartPutBeforeConnectFails(IpcOption option) throws Exception {
    final int unusedPort;
    try (final ServerSocket socket = new ServerSocket(0)) {
      unusedPort = socket.getLocalPort();
    }
    try (final FlightClient unconnected = newClient(unusedPort);
        final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.ClientStreamListener writer =
          assertTimeoutPreemptively(
              Duration.ofSeconds(30),
              () -> {
                final FlightClient.ClientStreamListener listener =
                    unconnected.startPut(
                        FlightDescriptor.command(COMMAND),
                        new AsyncPutListener() {
                          @Override
                          public boolean isCancelled() {
                            return false;
                          }
                        });
                listener.start(root, provider, option);
                return listener;
              });
      writer.completed();
      final FlightRuntimeException e =
          assertThrows(FlightRuntimeException.class, writer::getResult);
      assertEquals(FlightStatusCode.UNAVAILABLE, e.status().code());
    }
  }

  /**
   * Goal: no additional copy. Serializing a message wraps its body buffers, compressed or not, in
   * the stream handed to gRPC instead of copying them, with or without zero-copy writes (without,
   * gRPC copies the stream into its own buffers as it always has).
   */
  @ParameterizedTest
  @MethodSource("optionsAndZeroCopy")
  public void testSerializeDoesNotCopyBody(IpcOption option, boolean zeroCopy) throws Exception {
    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (final VectorSchemaRoot root = createRoot(allocator)) {
      // The body: the compressed buffers, or the vectors' own buffers.
      final ArrowRecordBatch batch = recordBatch(root, option);
      final List<ArrowBuf> body = batch.getBuffers();
      try (final ArrowMessage message =
          new ArrowMessage(batch, /* appMetadata */ null, zeroCopy, option)) {
        final long allocated = allocator.getAllocatedMemory();
        try (final InputStream stream = marshaller.stream(message)) {
          assertEquals(allocated, allocator.getAllocatedMemory());
          final List<Long> streamed = componentAddresses(stream);
          for (ArrowBuf buf : body) {
            if (buf.readableBytes() > 0) {
              assertTrue(streamed.contains(buf.memoryAddress()), "Body buffer was copied");
            }
          }
        }
      }
    }
  }

  /** Serialize a message, as gRPC would, and check it can be read back. */
  private void verifySerialized(
      MethodDescriptor.Marshaller<ArrowMessage> marshaller, ArrowMessage message) throws Exception {
    try (final InputStream stream = marshaller.stream(message);
        final ArrowMessage parsed = marshaller.parse(stream)) {
      assertEquals(message.getMessageType(), parsed.getMessageType());
      if (parsed.getMessageType() == ArrowMessage.HeaderType.RECORD_BATCH) {
        verifyBatch(parsed);
      }
    }
  }

  /** The memory addresses of the buffers making up a serialized message. */
  private static List<Long> componentAddresses(InputStream stream) throws Exception {
    final java.lang.reflect.Field field = stream.getClass().getDeclaredField("buf");
    field.setAccessible(true);
    final CompositeByteBuf buf = (CompositeByteBuf) field.get(stream);
    final List<Long> addresses = new ArrayList<>();
    for (int i = 0; i < buf.numComponents(); i++) {
      final ByteBuf component = buf.internalComponent(i);
      if (component.hasMemoryAddress()) {
        addresses.add(component.memoryAddress() + component.readerIndex());
      }
    }
    return addresses;
  }

  /**
   * A client call's write side that queues the messages handed to it, as gRPC does before a call is
   * connected and until it is committed (for retries), and that is ready only once told so.
   */
  private static final class MessageQueueingObserver
      extends ClientCallStreamObserver<ArrowMessage> {
    final List<ArrowMessage> messages = new ArrayList<>();
    volatile boolean ready = false;

    @Override
    public void onNext(ArrowMessage value) {
      messages.add(value);
    }

    @Override
    public boolean isReady() {
      return ready;
    }

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onCompleted() {}

    @Override
    public void cancel(String message, Throwable cause) {}

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int count) {}

    @Override
    public void setMessageCompression(boolean enable) {}
  }

  private static ArrowRecordBatch recordBatch(VectorSchemaRoot root, IpcOption option) {
    final CompressionCodec codec = CompressionUtil.createCodec(option);
    return new VectorUnloader(root, /* includeNullCount */ true, codec, /* alignBuffers */ true)
        .getRecordBatch();
  }

  /** Verify a parsed record batch written from {@link #createRoot}. */
  private void verifyBatch(ArrowMessage parsed) throws Exception {
    try (final VectorSchemaRoot received = VectorSchemaRoot.create(SCHEMA, allocator);
        final ArrowRecordBatch batch = parsed.asRecordBatch()) {
      new VectorLoader(received).load(batch);
      final IntVector ints = (IntVector) received.getVector("i");
      for (int row = 0; row < ROWS; row++) {
        assertEquals(row % 16, ints.get(row));
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

  /** Goal: clients compress too. A client writing DoPut compresses both kinds of batch. */
  @ParameterizedTest
  @MethodSource("options")
  public void testClientWireFormat(IpcOption option) throws Exception {
    final List<Flight.FlightData> messages = Collections.synchronizedList(new ArrayList<>());
    final String name = InProcessServerBuilder.generateName();
    final Server rawServer =
        InProcessServerBuilder.forName(name)
            .addService(
                new FlightServiceGrpc.FlightServiceImplBase() {
                  @Override
                  public StreamObserver<Flight.FlightData> doPut(
                      StreamObserver<Flight.PutResult> responseObserver) {
                    return new StreamObserver<Flight.FlightData>() {
                      @Override
                      public void onNext(Flight.FlightData value) {
                        messages.add(value);
                      }

                      @Override
                      public void onError(Throwable t) {}

                      @Override
                      public void onCompleted() {
                        responseObserver.onCompleted();
                      }
                    };
                  }
                })
            .build()
            .start();
    try (final FlightClient rawClient =
            FlightGrpcUtils.createFlightClient(
                allocator, InProcessChannelBuilder.forName(name).build());
        final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.ClientStreamListener listener =
          rawClient.startPut(FlightDescriptor.command(COMMAND), new AsyncPutListener());
      writeStream(listener, root, provider, option);
      listener.getResult();
    } finally {
      rawServer.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    }
    verifyWireFormat(option, messages);
  }

  /** DoPut the root with the given option, which the producer verifies. */
  private void putStream(FlightClient client, IpcOption option, boolean zeroCopy) throws Exception {
    producer.verifiedPutBatches = 0;
    try (final DictionaryProvider.MapDictionaryProvider provider = createProvider(allocator);
        final VectorSchemaRoot root = createRoot(allocator)) {
      final FlightClient.ClientStreamListener listener =
          client.startPut(FlightDescriptor.command(COMMAND), new AsyncPutListener());
      listener.setUseZeroCopy(zeroCopy);
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
    fillRoot(root);
    return root;
  }

  private static void fillRoot(VectorSchemaRoot root) {
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
