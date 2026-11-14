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
package org.apache.arrow.compression;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that {@link ArrowStreamWriter} and {@link ArrowFileWriter} honor the compression settings
 * of an {@link IpcOption} passed to their option-only constructors.
 */
public class TestIpcOptionCompression {

  private static final int ROW_COUNT = 10_000;

  private BufferAllocator allocator;

  @BeforeEach
  public void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  static Stream<IpcOption> compressedOptions() {
    return Stream.of(
        IpcOption.DEFAULT.withCompression(CodecType.ZSTD),
        IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 1),
        IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 19),
        IpcOption.DEFAULT.withCompression(CodecType.LZ4_FRAME));
  }

  private interface WriterFactory {
    ArrowWriter create(VectorSchemaRoot root, ByteArrayOutputStream out);
  }

  private VectorSchemaRoot createRoot() {
    IntVector vector = new IntVector("ints", allocator);
    vector.allocateNew(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      vector.set(i, i % 7); // highly compressible
    }
    vector.setValueCount(ROW_COUNT);
    return new VectorSchemaRoot(
        new Schema(Collections.singletonList(vector.getField())),
        Collections.singletonList(vector),
        ROW_COUNT);
  }

  private byte[] write(WriterFactory factory) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = createRoot();
        ArrowWriter writer = factory.create(root, out)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  private static void assertReadsBack(ArrowReader reader) throws IOException {
    assertTrue(reader.loadNextBatch());
    IntVector vector = (IntVector) reader.getVectorSchemaRoot().getVector(0);
    assertEquals(ROW_COUNT, vector.getValueCount());
    for (int i = 0; i < ROW_COUNT; i++) {
      assertEquals(i % 7, vector.get(i));
    }
    assertFalse(reader.loadNextBatch());
  }

  @ParameterizedTest
  @MethodSource("compressedOptions")
  public void testStreamWriterHonorsOptionCompression(IpcOption option) throws IOException {
    byte[] compressed =
        write((root, out) -> new ArrowStreamWriter(root, null, Channels.newChannel(out), option));
    byte[] uncompressed =
        write(
            (root, out) ->
                new ArrowStreamWriter(root, null, Channels.newChannel(out), IpcOption.DEFAULT));
    assertTrue(
        compressed.length < uncompressed.length,
        "compressed " + compressed.length + " vs uncompressed " + uncompressed.length);

    // Round-trips with an explicit codec factory and with the ServiceLoader default.
    for (CompressionCodec.Factory factory :
        List.of(CommonsCompressionFactory.INSTANCE, CompressionCodec.Factory.INSTANCE)) {
      try (ArrowStreamReader reader =
          new ArrowStreamReader(new ByteArrayInputStream(compressed), allocator, factory)) {
        assertReadsBack(reader);
      }
    }

    // The body really is compressed: a reader without a codec cannot load it.
    try (ArrowStreamReader reader =
        new ArrowStreamReader(
            new ByteArrayInputStream(compressed), allocator, NoCompressionCodec.Factory.INSTANCE)) {
      assertThrows(IllegalArgumentException.class, reader::loadNextBatch);
    }
  }

  @ParameterizedTest
  @MethodSource("compressedOptions")
  public void testFileWriterHonorsOptionCompression(IpcOption option) throws IOException {
    byte[] compressed =
        write((root, out) -> new ArrowFileWriter(root, null, Channels.newChannel(out), option));
    byte[] uncompressed =
        write(
            (root, out) ->
                new ArrowFileWriter(root, null, Channels.newChannel(out), IpcOption.DEFAULT));
    assertTrue(
        compressed.length < uncompressed.length,
        "compressed " + compressed.length + " vs uncompressed " + uncompressed.length);

    try (ArrowFileReader reader =
        new ArrowFileReader(
            new ByteArrayReadableSeekableByteChannel(compressed),
            allocator,
            CommonsCompressionFactory.INSTANCE)) {
      assertReadsBack(reader);
    }

    try (ArrowFileReader reader =
        new ArrowFileReader(
            new ByteArrayReadableSeekableByteChannel(compressed),
            allocator,
            NoCompressionCodec.Factory.INSTANCE)) {
      assertThrows(IllegalArgumentException.class, reader::loadNextBatch);
    }

    // The metadata-taking constructor honors the option as well.
    byte[] withMetadata =
        write(
            (root, out) ->
                new ArrowFileWriter(
                    root, null, Channels.newChannel(out), Collections.emptyMap(), option));
    try (ArrowFileReader reader =
        new ArrowFileReader(
            new ByteArrayReadableSeekableByteChannel(withMetadata),
            allocator,
            CommonsCompressionFactory.INSTANCE)) {
      assertReadsBack(reader);
    }
  }

  @Test
  public void testOptionOnlyMatchesExplicitFactory() throws IOException {
    // The option-only constructor produces the same bytes as passing the codec explicitly.
    IpcOption option = IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 5);
    byte[] optionOnly =
        write((root, out) -> new ArrowStreamWriter(root, null, Channels.newChannel(out), option));
    byte[] explicit =
        write(
            (root, out) ->
                new ArrowStreamWriter(
                    root,
                    null,
                    Channels.newChannel(out),
                    option,
                    CommonsCompressionFactory.INSTANCE,
                    CodecType.ZSTD,
                    Optional.of(5)));
    assertArrayEquals(explicit, optionOnly);
  }

  @Test
  public void testConflictingExplicitCodecThrows() {
    IpcOption zstd = IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 5);
    try (VectorSchemaRoot root = createRoot()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowStreamWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  zstd,
                  CommonsCompressionFactory.INSTANCE,
                  CodecType.LZ4_FRAME,
                  Optional.of(5)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowFileWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  null,
                  zstd,
                  CommonsCompressionFactory.INSTANCE,
                  CodecType.ZSTD,
                  Optional.of(6)));
    }
  }
}
