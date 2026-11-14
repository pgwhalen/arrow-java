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
package org.apache.arrow.vector.ipc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.Test;

/** Tests how {@link ArrowWriter} applies the compression settings of an {@link IpcOption}. */
public class TestArrowWriterCompressionOption extends BaseFileTest {

  /** Identity "compression" reporting ZSTD, so the vector module needs no real codec. */
  static final class IdentityCodec extends AbstractCompressionCodec {
    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
      long len = uncompressedBuffer.writerIndex();
      ArrowBuf out = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + len);
      out.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, uncompressedBuffer, 0, len);
      out.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + len);
      return out;
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
      long len = compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
      ArrowBuf out = allocator.buffer(len);
      out.setBytes(0, compressedBuffer, CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, len);
      out.writerIndex(len);
      return out;
    }

    @Override
    public CodecType getCodecType() {
      return CodecType.ZSTD;
    }
  }

  static final CompressionCodec.Factory IDENTITY_FACTORY =
      new CompressionCodec.Factory() {
        @Override
        public CompressionCodec createCodec(CodecType codecType) {
          return codecType == CodecType.NO_COMPRESSION
              ? NoCompressionCodec.INSTANCE
              : new IdentityCodec();
        }

        @Override
        public CompressionCodec createCodec(CodecType codecType, int compressionLevel) {
          return createCodec(codecType);
        }
      };

  private interface WriterFactory {
    ArrowWriter create(VectorSchemaRoot root, ByteArrayOutputStream out);
  }

  private VectorSchemaRoot createRoot() {
    IntVector vector = new IntVector("foo", allocator);
    vector.allocateNew(3);
    vector.set(0, 1);
    vector.set(1, 2);
    vector.set(2, 3);
    vector.setValueCount(3);
    Schema schema = new Schema(Collections.singletonList(vector.getField()));
    return new VectorSchemaRoot(schema, Collections.singletonList(vector), 3);
  }

  private byte[] write(WriterFactory factory) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = createRoot();
        ArrowWriter writer = factory.create(root, out)) {
      writer.start();
      writer.writeBatch();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  /** Version and body compression codec (or null) of each record batch message of a stream. */
  private List<String> describeRecordBatches(byte[] stream) throws IOException {
    List<String> result = new ArrayList<>();
    try (MessageChannelReader reader =
        new MessageChannelReader(
            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(stream))), allocator)) {
      MessageResult messageResult;
      while ((messageResult = reader.readNext()) != null) {
        Message message = messageResult.getMessage();
        if (messageResult.getBodyBuffer() != null) {
          messageResult.getBodyBuffer().close();
        }
        if (message.headerType() == MessageHeader.RecordBatch) {
          RecordBatch batch = (RecordBatch) message.header(new RecordBatch());
          result.add(
              org.apache.arrow.flatbuf.MetadataVersion.name(message.version())
                  + "/"
                  + (batch.compression() == null ? null : batch.compression().codec()));
        }
      }
    }
    return result;
  }

  @Test
  public void testUncompressedOptionsAreByteIdenticalToExplicitNoCompression() throws IOException {
    List<IpcOption> options =
        List.of(
            IpcOption.DEFAULT,
            new IpcOption(),
            new IpcOption(false, MetadataVersion.V4),
            new IpcOption(true, MetadataVersion.V4),
            new IpcOption(true, MetadataVersion.V5),
            IpcOption.DEFAULT.withCompression(CodecType.NO_COMPRESSION),
            new IpcOption(true, MetadataVersion.V4).withCompression(CodecType.NO_COMPRESSION));
    for (IpcOption option : options) {
      byte[] streamOptionOnly =
          write((root, out) -> new ArrowStreamWriter(root, null, Channels.newChannel(out), option));
      byte[] streamExplicit =
          write(
              (root, out) ->
                  new ArrowStreamWriter(
                      root,
                      null,
                      Channels.newChannel(out),
                      option,
                      NoCompressionCodec.Factory.INSTANCE,
                      CodecType.NO_COMPRESSION,
                      Optional.empty()));
      assertArrayEquals(streamExplicit, streamOptionOnly);

      byte[] fileOptionOnly =
          write((root, out) -> new ArrowFileWriter(root, null, Channels.newChannel(out), option));
      byte[] fileExplicit =
          write(
              (root, out) ->
                  new ArrowFileWriter(
                      root,
                      null,
                      Channels.newChannel(out),
                      null,
                      option,
                      NoCompressionCodec.Factory.INSTANCE,
                      CodecType.NO_COMPRESSION,
                      Optional.empty()));
      assertArrayEquals(fileExplicit, fileOptionOnly);
    }

    // The constructors without an option are the same as DEFAULT.
    assertArrayEquals(
        write((root, out) -> new ArrowStreamWriter(root, null, Channels.newChannel(out))),
        write(
            (root, out) ->
                new ArrowStreamWriter(root, null, Channels.newChannel(out), IpcOption.DEFAULT)));
    assertArrayEquals(
        write((root, out) -> new ArrowFileWriter(root, null, Channels.newChannel(out))),
        write(
            (root, out) ->
                new ArrowFileWriter(root, null, Channels.newChannel(out), IpcOption.DEFAULT)));
  }

  @Test
  public void testOptionCompressionWithoutCodecOnClasspathFails() {
    // arrow-compression is not on the vector module's test classpath, so the best-effort factory
    // cannot create ZSTD: the writer must fail rather than silently write uncompressed data.
    IpcOption zstd = IpcOption.DEFAULT.withCompression(CodecType.ZSTD);
    try (VectorSchemaRoot root = createRoot()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertThrows(
          IllegalArgumentException.class,
          () -> new ArrowStreamWriter(root, null, Channels.newChannel(out), zstd));
      assertThrows(
          IllegalArgumentException.class,
          () -> new ArrowFileWriter(root, null, Channels.newChannel(out), zstd));
    }
  }

  @Test
  public void testExplicitArgsConflictingWithOptionThrow() {
    IpcOption zstd = IpcOption.DEFAULT.withCompression(CodecType.ZSTD);
    IpcOption zstd3 = IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 3);
    try (VectorSchemaRoot root = createRoot()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // different codec
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  new ArrowStreamWriter(
                      root,
                      null,
                      Channels.newChannel(out),
                      zstd,
                      IDENTITY_FACTORY,
                      CodecType.LZ4_FRAME));
      assertTrue(e.getMessage().contains("conflicts"), e.getMessage());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowStreamWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  zstd,
                  NoCompressionCodec.Factory.INSTANCE,
                  CodecType.NO_COMPRESSION));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowFileWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  null,
                  zstd,
                  IDENTITY_FACTORY,
                  CodecType.LZ4_FRAME));
      // same codec, different level
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowStreamWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  zstd3,
                  IDENTITY_FACTORY,
                  CodecType.ZSTD,
                  Optional.of(4)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowStreamWriter(
                  root, null, Channels.newChannel(out), zstd3, IDENTITY_FACTORY, CodecType.ZSTD));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ArrowFileWriter(
                  root,
                  null,
                  Channels.newChannel(out),
                  null,
                  zstd,
                  IDENTITY_FACTORY,
                  CodecType.ZSTD,
                  Optional.of(3)));
    }
  }

  @Test
  public void testExplicitArgsAgreeingWithOptionUseExplicitFactory() throws IOException {
    IpcOption zstd3 = IpcOption.DEFAULT.withCompression(CodecType.ZSTD, 3);
    byte[] stream =
        write(
            (root, out) ->
                new ArrowStreamWriter(
                    root,
                    null,
                    Channels.newChannel(out),
                    zstd3,
                    IDENTITY_FACTORY,
                    CodecType.ZSTD,
                    Optional.of(3)));
    assertEquals(List.of("V5/1", "V5/1"), describeRecordBatches(stream));
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(stream), allocator, IDENTITY_FACTORY)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(2, ((IntVector) reader.getVectorSchemaRoot().getVector(0)).get(1));
    }

    byte[] file =
        write(
            (root, out) ->
                new ArrowFileWriter(
                    root,
                    null,
                    Channels.newChannel(out),
                    null,
                    zstd3,
                    IDENTITY_FACTORY,
                    CodecType.ZSTD,
                    Optional.of(3)));
    try (ArrowFileReader reader =
        new ArrowFileReader(
            new ByteArrayReadableSeekableByteChannel(file), allocator, IDENTITY_FACTORY)) {
      assertEquals(2, reader.getRecordBlocks().size());
      assertTrue(reader.loadNextBatch());
      assertEquals(3, ((IntVector) reader.getVectorSchemaRoot().getVector(0)).get(2));
    }
  }

  /**
   * Regression test: body compression is a V5 feature, so an option that requests compression
   * cannot be combined with V4 metadata or the legacy format. Previously {@code new
   * IpcOption(false, V4).withCompression(ZSTD)} was accepted and the writers produced record
   * batches stamped V4 that nevertheless carried a body compression field and length-prefixed
   * (compressed) buffers.
   */
  @Test
  public void testCompressionRejectedForV4AndLegacyFormat() throws IOException {
    for (IpcOption base :
        List.of(
            new IpcOption(false, MetadataVersion.V4),
            new IpcOption(true, MetadataVersion.V4),
            new IpcOption(true, MetadataVersion.V5))) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> base.withCompression(CodecType.ZSTD));
      assertNotNull(e.getMessage());
      assertThrows(
          IllegalArgumentException.class, () -> base.withCompression(CodecType.LZ4_FRAME, 1));
    }

    // With V5 and the current format, the same writer output is stamped V5.
    byte[] stream =
        write(
            (root, out) ->
                new ArrowStreamWriter(
                    root,
                    null,
                    Channels.newChannel(out),
                    new IpcOption(false, MetadataVersion.V5).withCompression(CodecType.ZSTD),
                    IDENTITY_FACTORY,
                    CodecType.ZSTD));
    assertEquals(List.of("V5/1", "V5/1"), describeRecordBatches(stream));
  }
}
