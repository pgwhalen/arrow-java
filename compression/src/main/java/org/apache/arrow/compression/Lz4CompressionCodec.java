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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz77support.Parameters;
import org.apache.commons.compress.utils.IOUtils;

/** Compression codec for the LZ4 algorithm. */
public class Lz4CompressionCodec extends AbstractCompressionCodec {

  /** Lowest accepted compression level (liblz4's "default" level). */
  static final int MIN_COMPRESSION_LEVEL = 0;

  /** Lowest level that selects high-compression mode (liblz4's {@code LZ4HC_CLEVEL_MIN}). */
  static final int MIN_HC_COMPRESSION_LEVEL = 3;

  /** Highest accepted compression level (liblz4's {@code LZ4HC_CLEVEL_MAX}). */
  static final int MAX_COMPRESSION_LEVEL = 12;

  /**
   * Maximum number of match candidates examined per position for levels {@value
   * #MIN_HC_COMPRESSION_LEVEL}..{@value #MAX_COMPRESSION_LEVEL}. Strictly increasing, starting
   * above commons-compress's default (512) and ending at the search depth liblz4 uses for its
   * maximum level (16384).
   */
  private static final int[] HC_MAX_CANDIDATES = {
    768, 1024, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384
  };

  private final int compressionLevel;
  // The LZ77 match-finder settings for the level, or null for the commons-compress defaults.
  private final Parameters lz77Parameters;

  /**
   * Creates a codec using the default LZ4 frame parameters of commons-compress (equivalent to
   * compression level 0), except for the frame's max block size.
   *
   * <p>Each buffer is compressed as its own frame, whose max block size is the smallest of 64 KiB,
   * 256 KiB, 1 MiB and 4 MiB that holds the whole buffer. The commons-compress default is always 4
   * MiB, which allocates a 4 MiB block for every buffer, including small validity buffers.
   */
  public Lz4CompressionCodec() {
    this.compressionLevel = MIN_COMPRESSION_LEVEL;
    this.lz77Parameters = null;
  }

  /**
   * Creates a codec with the given compression level, following the semantics of the liblz4 frame
   * API ({@code LZ4F_preferences_t.compressionLevel}) that Arrow C++ and PyArrow use.
   *
   * <p>Compression is implemented with commons-compress, which has no numeric level, so the level
   * is mapped onto its LZ77 match-finder settings. The output is always a standard LZ4 frame
   * (independent blocks, content checksum, and a max block size chosen from the buffer size as
   * described on {@link #Lz4CompressionCodec()}), identical in layout to the frame produced by
   * {@link #Lz4CompressionCodec()}, so it can be decoded by any LZ4 frame decoder. The mapping is
   * monotonic: a higher level never searches less.
   *
   * <ul>
   *   <li>{@code 0..2}: fast mode. Uses the commons-compress defaults (512 match candidates per
   *       position, lazy matching); output is byte-identical to {@link #Lz4CompressionCodec()}.
   *       liblz4 likewise treats every level below 3 as its default fast mode.
   *   <li>{@code 3..12}: high-compression mode (liblz4's LZ4HC range). Lazy matching with an
   *       increasing number of match candidates examined per position: 768, 1024, 1536, 2048, 3072,
   *       4096, 6144, 8192, 12288, 16384 for levels 3 through 12 respectively (16384 is also
   *       liblz4's search depth at level 12). Higher levels can be slower on inputs with many
   *       candidate matches and typically produce smaller output (how much smaller depends on the
   *       data; on some inputs several levels give identical output).
   * </ul>
   *
   * <p>Levels outside {@code 0..12} are rejected rather than silently clamped. Negative levels
   * select liblz4's "acceleration" mode, which trades ratio for speed and has no equivalent here;
   * levels above 12 exceed liblz4's maximum.
   *
   * @param compressionLevel the compression level, in the range {@code 0..12}
   * @throws IllegalArgumentException if {@code compressionLevel} is outside {@code 0..12}
   */
  public Lz4CompressionCodec(int compressionLevel) {
    Preconditions.checkArgument(
        compressionLevel >= MIN_COMPRESSION_LEVEL && compressionLevel <= MAX_COMPRESSION_LEVEL,
        "LZ4 compression level must be between %s and %s, got %s",
        MIN_COMPRESSION_LEVEL,
        MAX_COMPRESSION_LEVEL,
        compressionLevel);
    this.compressionLevel = compressionLevel;
    this.lz77Parameters =
        compressionLevel < MIN_HC_COMPRESSION_LEVEL
            ? null
            : BlockLZ4CompressorOutputStream.createParameterBuilder()
                .withLazyMatching(true)
                .withMaxNumberOfCandidates(
                    HC_MAX_CANDIDATES[compressionLevel - MIN_HC_COMPRESSION_LEVEL])
                .build();
  }

  /** The frame parameters for a buffer of the given length. */
  private FramedLZ4CompressorOutputStream.Parameters frameParameters(int length) {
    final FramedLZ4CompressorOutputStream.BlockSize blockSize = blockSize(length);
    // The other frame settings are those of Parameters.DEFAULT.
    return lz77Parameters == null
        ? new FramedLZ4CompressorOutputStream.Parameters(blockSize)
        : new FramedLZ4CompressorOutputStream.Parameters(
            blockSize,
            /* withContentChecksum= */ true,
            /* withBlockChecksum= */ false,
            /* withBlockDependency= */ false,
            lz77Parameters);
  }

  /** The smallest LZ4 frame max block size that holds {@code length} bytes, up to 4 MiB. */
  static FramedLZ4CompressorOutputStream.BlockSize blockSize(int length) {
    if (length <= 64 * 1024) {
      return FramedLZ4CompressorOutputStream.BlockSize.K64;
    }
    if (length <= 256 * 1024) {
      return FramedLZ4CompressorOutputStream.BlockSize.K256;
    }
    if (length <= 1024 * 1024) {
      return FramedLZ4CompressorOutputStream.BlockSize.M1;
    }
    return FramedLZ4CompressorOutputStream.BlockSize.M4;
  }

  /** The compression level this codec was created with; visible for testing. */
  int getCompressionLevel() {
    return compressionLevel;
  }

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    Preconditions.checkArgument(
        uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The uncompressed buffer size exceeds the integer limit %s.",
        Integer.MAX_VALUE);

    byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
    uncompressedBuffer.getBytes(/* index= */ 0, inBytes);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream in = new ByteArrayInputStream(inBytes);
        OutputStream out =
            new FramedLZ4CompressorOutputStream(baos, frameParameters(inBytes.length))) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = baos.toByteArray();

    ArrowBuf compressedBuffer =
        allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    compressedBuffer.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    return compressedBuffer;
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(
        compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The compressed buffer size exceeds the integer limit %s",
        Integer.MAX_VALUE);

    long decompressedLength = readUncompressedLength(compressedBuffer);

    byte[] inBytes =
        new byte
            [(int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
    compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
    try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(inBytes))) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = out.toByteArray();
    if (outBytes.length != decompressedLength) {
      throw new RuntimeException(
          "Expected != actual decompressed length: "
              + decompressedLength
              + " != "
              + outBytes.length);
    }
    ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
    decompressedBuffer.setBytes(/* index= */ 0, outBytes);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }
}
