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

import java.util.Optional;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.types.MetadataVersion;
import org.junit.jupiter.api.Test;

public class TestIpcOption {

  @Test
  public void testDefaultIsUncompressed() {
    assertEquals(CodecType.NO_COMPRESSION, IpcOption.DEFAULT.getCodecType());
    assertFalse(IpcOption.DEFAULT.getCompressionLevel().isPresent());
  }

  @Test
  public void testWithCompression() {
    final IpcOption base = new IpcOption(true, MetadataVersion.V4);

    final IpcOption zstd = base.withCompression(CodecType.ZSTD);
    assertEquals(CodecType.ZSTD, zstd.getCodecType());
    assertFalse(zstd.getCompressionLevel().isPresent());
    assertEquals(true, zstd.write_legacy_ipc_format);
    assertEquals(MetadataVersion.V4, zstd.metadataVersion);

    final IpcOption leveled = zstd.withCompression(CodecType.LZ4_FRAME, 3);
    assertEquals(CodecType.LZ4_FRAME, leveled.getCodecType());
    assertEquals(Optional.of(3), leveled.getCompressionLevel());

    // Instances are immutable
    assertEquals(CodecType.NO_COMPRESSION, base.getCodecType());
    assertEquals(CodecType.ZSTD, zstd.getCodecType());
  }
}
