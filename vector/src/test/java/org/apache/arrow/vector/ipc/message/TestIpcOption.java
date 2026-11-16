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
package org.apache.arrow.vector.ipc.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.types.MetadataVersion;
import org.junit.jupiter.api.Test;

public class TestIpcOption {

  @Test
  public void testDefaultIsUncompressed() {
    assertEquals(CodecType.NO_COMPRESSION, IpcOption.DEFAULT.codecType);
    assertFalse(IpcOption.DEFAULT.compressionLevel.isPresent());
  }

  @Test
  public void testWithCompression() {
    final IpcOption base = new IpcOption(false, MetadataVersion.V5);

    final IpcOption zstd = base.withCompression(CodecType.ZSTD);
    assertEquals(CodecType.ZSTD, zstd.codecType);
    assertFalse(zstd.compressionLevel.isPresent());
    assertFalse(zstd.write_legacy_ipc_format);
    assertEquals(MetadataVersion.V5, zstd.metadataVersion);

    final IpcOption leveled = zstd.withCompression(CodecType.ZSTD, 3);
    assertEquals(CodecType.ZSTD, leveled.codecType);
    assertEquals(Optional.of(3), leveled.compressionLevel);

    // Switching codec clears the level
    final IpcOption lz4 = leveled.withCompression(CodecType.LZ4_FRAME);
    assertEquals(CodecType.LZ4_FRAME, lz4.codecType);
    assertFalse(lz4.compressionLevel.isPresent());

    // NO_COMPRESSION turns compression off again and clears the level
    final IpcOption off = leveled.withCompression(CodecType.NO_COMPRESSION);
    assertEquals(CodecType.NO_COMPRESSION, off.codecType);
    assertFalse(off.compressionLevel.isPresent());

    // Instances are immutable
    assertEquals(CodecType.NO_COMPRESSION, base.codecType);
    assertEquals(CodecType.ZSTD, zstd.codecType);
  }

  /**
   * As in C++, compression combines with any metadata version and IPC format, and keeps them as
   * they are.
   */
  @Test
  public void testCompressionWithV4AndLegacyFormat() {
    for (IpcOption base :
        new IpcOption[] {
          new IpcOption(false, MetadataVersion.V4),
          new IpcOption(true, MetadataVersion.V5),
          new IpcOption(true, MetadataVersion.V4)
        }) {
      final IpcOption zstd = base.withCompression(CodecType.ZSTD, 1);
      assertEquals(CodecType.ZSTD, zstd.codecType);
      assertEquals(Optional.of(1), zstd.compressionLevel);
      assertEquals(base.metadataVersion, zstd.metadataVersion);
      assertEquals(base.write_legacy_ipc_format, zstd.write_legacy_ipc_format);
    }
  }

  /** A level is kept for any codec; the codec factory decides what it means. */
  @Test
  public void testLevelKeptForAnyCodec() {
    for (CodecType codecType : CodecType.values()) {
      final IpcOption option = IpcOption.DEFAULT.withCompression(codecType, 3);
      assertEquals(codecType, option.codecType);
      assertEquals(Optional.of(3), option.compressionLevel);
    }
  }

  @Test
  public void testNullCodecRejected() {
    assertThrows(NullPointerException.class, () -> IpcOption.DEFAULT.withCompression(null));
    assertThrows(NullPointerException.class, () -> IpcOption.DEFAULT.withCompression(null, 1));
  }
}
