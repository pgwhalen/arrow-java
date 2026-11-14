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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    final IpcOption leveled = zstd.withCompression(CodecType.LZ4_FRAME, 3);
    assertEquals(CodecType.LZ4_FRAME, leveled.codecType);
    assertEquals(Optional.of(3), leveled.compressionLevel);

    // NO_COMPRESSION turns compression off again and clears the level
    final IpcOption off = leveled.withCompression(CodecType.NO_COMPRESSION);
    assertEquals(CodecType.NO_COMPRESSION, off.codecType);
    assertFalse(off.compressionLevel.isPresent());

    // Instances are immutable
    assertEquals(CodecType.NO_COMPRESSION, base.codecType);
    assertEquals(CodecType.ZSTD, zstd.codecType);
  }

  @Test
  public void testCompressionRequiresV5() {
    final IpcOption v4 = new IpcOption(false, MetadataVersion.V4);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> v4.withCompression(CodecType.ZSTD));
    assertTrue(e.getMessage().contains("V5"), e.getMessage());
    assertThrows(IllegalArgumentException.class, () -> v4.withCompression(CodecType.LZ4_FRAME, 1));

    // Disabling compression is always allowed
    assertEquals(CodecType.NO_COMPRESSION, v4.withCompression(CodecType.NO_COMPRESSION).codecType);
  }

  @Test
  public void testCompressionRejectsLegacyFormat() {
    final IpcOption legacy = new IpcOption(true, MetadataVersion.V5);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> legacy.withCompression(CodecType.ZSTD));
    assertTrue(e.getMessage().contains("legacy"), e.getMessage());
    assertThrows(
        IllegalArgumentException.class, () -> legacy.withCompression(CodecType.LZ4_FRAME, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new IpcOption(true, MetadataVersion.V4).withCompression(CodecType.ZSTD));

    assertEquals(
        CodecType.NO_COMPRESSION, legacy.withCompression(CodecType.NO_COMPRESSION).codecType);
  }

  @Test
  public void testLevelRejectedForNoCompression() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IpcOption.DEFAULT.withCompression(CodecType.NO_COMPRESSION, 3));
    assertTrue(e.getMessage().contains("NO_COMPRESSION"), e.getMessage());
  }

  @Test
  public void testNullCodecRejected() {
    assertThrows(NullPointerException.class, () -> IpcOption.DEFAULT.withCompression(null));
  }
}
