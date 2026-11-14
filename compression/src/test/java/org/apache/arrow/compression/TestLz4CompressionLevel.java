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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for the compression level support of {@link Lz4CompressionCodec}. */
class TestLz4CompressionLevel {

  private static final int FRAME_HEADER_LENGTH = 7; // magic (4) + FLG + BD + HC, no content size

  private BufferAllocator allocator;

  @BeforeEach
  void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  void terminate() {
    allocator.close();
  }

  /** Text built from a small vocabulary of words in a seeded random order. */
  static byte[] wordData(int size) {
    Random random = new Random(42);
    String[] vocabulary = new String[200];
    for (int i = 0; i < vocabulary.length; i++) {
      StringBuilder word = new StringBuilder();
      int length = 2 + random.nextInt(8);
      for (int j = 0; j < length; j++) {
        word.append((char) ('a' + random.nextInt(6)));
      }
      vocabulary[i] = word.toString();
    }
    StringBuilder sb = new StringBuilder(size + 16);
    while (sb.length() < size) {
      sb.append(vocabulary[random.nextInt(vocabulary.length)]).append(' ');
    }
    return Arrays.copyOf(sb.toString().getBytes(StandardCharsets.US_ASCII), size);
  }

  static byte[] randomData(int size) {
    byte[] data = new byte[size];
    new Random(7).nextBytes(data);
    return data;
  }

  /**
   * Seeded random bytes drawn from {0, 1}: compressible but not trivially so, and every position
   * has thousands of competing match candidates, so the match-finder effort matters.
   */
  static byte[] binaryAlphabetData(int size) {
    Random random = new Random(1);
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) random.nextInt(2);
    }
    return data;
  }

  /** Little-endian 32-bit ints with a seeded half-normal distribution, like a typical column. */
  static byte[] gaussianIntData(int count) {
    Random random = new Random(3);
    byte[] data = new byte[count * 4];
    for (int i = 0; i < count; i++) {
      int value = (int) Math.abs(random.nextGaussian() * 300);
      data[i * 4] = (byte) value;
      data[i * 4 + 1] = (byte) (value >>> 8);
    }
    return data;
  }

  static byte[] intData(int count) {
    byte[] data = new byte[count * 4];
    for (int i = 0; i < count; i++) {
      int value = i % 1000;
      data[i * 4] = (byte) value;
      data[i * 4 + 1] = (byte) (value >>> 8);
    }
    return data;
  }

  /**
   * {@link #intData} truncated to {@code length} bytes: compressible, and unlike {@link #wordData}
   * fast to compress at large sizes.
   */
  static byte[] intBytes(int length) {
    return Arrays.copyOf(intData((length + 3) / 4), length);
  }

  static Stream<Arguments> levelsAndData() {
    byte[][] inputs = {
      new byte[0], new byte[] {1}, wordData(64 * 1024), randomData(10_000), intData(10_000)
    };
    return IntStream.rangeClosed(
            Lz4CompressionCodec.MIN_COMPRESSION_LEVEL, Lz4CompressionCodec.MAX_COMPRESSION_LEVEL)
        .boxed()
        .flatMap(level -> Arrays.stream(inputs).map(data -> Arguments.of(level, data)));
  }

  private byte[] compress(CompressionCodec codec, byte[] data) {
    ArrowBuf in = allocator.buffer(Math.max(1, data.length));
    in.setBytes(0, data);
    in.writerIndex(data.length);
    // compress() takes ownership of (and releases) the input buffer
    try (ArrowBuf out = codec.compress(allocator, in)) {
      byte[] result = new byte[(int) out.writerIndex()];
      out.getBytes(0, result);
      return result;
    }
  }

  private byte[] decompress(CompressionCodec codec, byte[] compressed) {
    ArrowBuf in = allocator.buffer(compressed.length);
    in.setBytes(0, compressed);
    in.writerIndex(compressed.length);
    try (ArrowBuf out = codec.decompress(allocator, in)) {
      byte[] result = new byte[(int) out.writerIndex()];
      out.getBytes(0, result);
      return result;
    }
  }

  private static long uncompressedLengthPrefix(byte[] compressedBuffer) {
    return ByteBuffer.wrap(compressedBuffer, 0, (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)
        .order(ByteOrder.LITTLE_ENDIAN)
        .getLong();
  }

  /** The LZ4 frame of a compressed Arrow buffer, i.e. without the uncompressed-length prefix. */
  private static byte[] frame(byte[] compressedBuffer) {
    return Arrays.copyOfRange(
        compressedBuffer,
        (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
        compressedBuffer.length);
  }

  @ParameterizedTest
  @MethodSource("levelsAndData")
  void testRoundTrip(int level, byte[] data) throws IOException {
    Lz4CompressionCodec codec = new Lz4CompressionCodec(level);
    byte[] compressed = compress(codec, data);
    assertArrayEquals(data, decompress(codec, compressed));
    // Any level is decodable by a codec created without a level...
    assertArrayEquals(data, decompress(new Lz4CompressionCodec(), compressed));
    if (uncompressedLengthPrefix(compressed) > 0) {
      // ...and, when actually compressed, is a plain LZ4 frame readable by a generic decoder.
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (InputStream in =
          new FramedLZ4CompressorInputStream(new ByteArrayInputStream(frame(compressed)))) {
        IOUtils.copy(in, out);
      }
      assertArrayEquals(data, out.toByteArray());
    }
  }

  @Test
  void testDefaultOutputUnchanged() throws IOException {
    // Above 1 MiB the frame keeps the commons-compress default 4 MiB block size, so the output is
    // what Lz4CompressionCodec produced before compression levels and block sizes were supported.
    byte[] data = intBytes(1024 * 1024 + 1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream out = new FramedLZ4CompressorOutputStream(baos)) {
      out.write(data);
    }
    byte[] expectedFrame = baos.toByteArray();

    assertArrayEquals(expectedFrame, frame(compress(new Lz4CompressionCodec(), data)));
    assertArrayEquals(
        expectedFrame,
        frame(
            compress(
                CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME),
                data)));
  }

  /**
   * Each buffer is compressed with the smallest frame block size that holds it, and otherwise the
   * commons-compress default frame parameters.
   */
  @ParameterizedTest
  @CsvSource({
    "1000, K64",
    "65536, K64",
    "65537, K256",
    "262144, K256",
    "262145, M1",
    "1048576, M1",
    "1048577, M4"
  })
  void testBlockSizeFitsBuffer(int length, FramedLZ4CompressorOutputStream.BlockSize expected)
      throws IOException {
    assertEquals(expected, Lz4CompressionCodec.blockSize(length));

    // Short int data does not compress, and is then stored uncompressed, without a frame.
    byte[] data = length < 4096 ? wordData(length) : intBytes(length);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream out =
        new FramedLZ4CompressorOutputStream(
            baos, new FramedLZ4CompressorOutputStream.Parameters(expected))) {
      out.write(data);
    }
    for (Lz4CompressionCodec codec :
        new Lz4CompressionCodec[] {
          new Lz4CompressionCodec(),
          new Lz4CompressionCodec(Lz4CompressionCodec.MAX_COMPRESSION_LEVEL)
        }) {
      byte[] compressed = compress(codec, data);
      // The frame descriptor's BD byte holds the block max size: 4 (64 KiB) to 7 (4 MiB).
      assertEquals(baos.toByteArray()[5], frame(compressed)[5]);
      assertArrayEquals(data, decompress(codec, compressed));
    }
    assertArrayEquals(baos.toByteArray(), frame(compress(new Lz4CompressionCodec(), data)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testFastLevelsMatchDefault(int level) {
    byte[] data = wordData(64 * 1024);
    byte[] expected = compress(new Lz4CompressionCodec(), data);
    assertArrayEquals(expected, compress(new Lz4CompressionCodec(level), data));
    assertArrayEquals(
        expected,
        compress(
            CommonsCompressionFactory.INSTANCE.createCodec(
                CompressionUtil.CodecType.LZ4_FRAME, level),
            data));
  }

  @Test
  void testHigherLevelsCompressBetter() {
    byte[] data = binaryAlphabetData(64 * 1024);
    byte[] defaultCompressed = compress(new Lz4CompressionCodec(), data);
    byte[] defaultHeader = Arrays.copyOf(frame(defaultCompressed), FRAME_HEADER_LENGTH);
    int previousSize = defaultCompressed.length;
    for (int level = Lz4CompressionCodec.MIN_HC_COMPRESSION_LEVEL;
        level <= Lz4CompressionCodec.MAX_COMPRESSION_LEVEL;
        level++) {
      byte[] compressed = compress(new Lz4CompressionCodec(level), data);
      assertTrue(
          compressed.length <= previousSize,
          "level " + level + " produced " + compressed.length + " > " + previousSize);
      previousSize = compressed.length;
      // Same frame header as the default codec: only the block contents differ.
      assertArrayEquals(defaultHeader, Arrays.copyOf(frame(compressed), FRAME_HEADER_LENGTH));
    }
    assertTrue(
        previousSize < defaultCompressed.length,
        "max level produced " + previousSize + ", default " + defaultCompressed.length);
  }

  @Test
  void testMaxLevelCompressesIntColumnBetter() {
    byte[] data = gaussianIntData(16 * 1024);
    int defaultSize = compress(new Lz4CompressionCodec(), data).length;
    int maxSize =
        compress(new Lz4CompressionCodec(Lz4CompressionCodec.MAX_COMPRESSION_LEVEL), data).length;
    assertTrue(maxSize < defaultSize, "max level produced " + maxSize + ", default " + defaultSize);
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 13, 100, Integer.MAX_VALUE})
  void testInvalidLevelsRejected(int level) {
    assertThrows(IllegalArgumentException.class, () -> new Lz4CompressionCodec(level));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CommonsCompressionFactory.INSTANCE.createCodec(
                CompressionUtil.CodecType.LZ4_FRAME, level));
  }

  @Test
  void testFactoryPassesLevelThrough() {
    for (int level = Lz4CompressionCodec.MIN_COMPRESSION_LEVEL;
        level <= Lz4CompressionCodec.MAX_COMPRESSION_LEVEL;
        level++) {
      CompressionCodec codec =
          CommonsCompressionFactory.INSTANCE.createCodec(
              CompressionUtil.CodecType.LZ4_FRAME, level);
      assertEquals(level, ((Lz4CompressionCodec) codec).getCompressionLevel());
    }
    assertEquals(
        0,
        ((Lz4CompressionCodec)
                CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME))
            .getCompressionLevel());

    byte[] data = binaryAlphabetData(64 * 1024);
    byte[] fast =
        compress(
            CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME, 1),
            data);
    byte[] max =
        compress(
            CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME, 12),
            data);
    assertFalse(Arrays.equals(fast, max));
    assertTrue(max.length < fast.length);
  }
}
