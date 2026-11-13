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

import java.util.Optional;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.types.MetadataVersion;

/** IPC options, now only use for write. */
public class IpcOption {

  // Write the pre-0.15.0 encapsulated IPC message format
  // consisting of a 4-byte prefix instead of 8 byte
  public final boolean write_legacy_ipc_format;

  // The metadata version. Defaults to V5.
  public final MetadataVersion metadataVersion;

  private final CompressionUtil.CodecType codecType;

  private final Optional<Integer> compressionLevel;

  public IpcOption() {
    this(false, MetadataVersion.DEFAULT);
  }

  /** Create options without body compression. */
  public IpcOption(boolean writeLegacyIpcFormat, MetadataVersion metadataVersion) {
    this(
        writeLegacyIpcFormat,
        metadataVersion,
        CompressionUtil.CodecType.NO_COMPRESSION,
        Optional.empty());
  }

  private IpcOption(
      boolean writeLegacyIpcFormat,
      MetadataVersion metadataVersion,
      CompressionUtil.CodecType codecType,
      Optional<Integer> compressionLevel) {
    this.write_legacy_ipc_format = writeLegacyIpcFormat;
    this.metadataVersion = metadataVersion;
    this.codecType = Preconditions.checkNotNull(codecType, "codecType must not be null");
    this.compressionLevel = compressionLevel;
  }

  public static final IpcOption DEFAULT = new IpcOption();

  /** The codec used to compress record batch and dictionary batch bodies. */
  public CompressionUtil.CodecType getCodecType() {
    return codecType;
  }

  /** The compression level, or empty to use the codec's default. */
  public Optional<Integer> getCompressionLevel() {
    return compressionLevel;
  }

  /**
   * Return a copy of these options that compresses record batch and dictionary batch bodies with
   * the given codec, like {@code IpcWriteOptions(compression=...)} in PyArrow.
   *
   * <p>Currently honored by Arrow Flight. Requires a codec implementation (e.g. the {@code
   * arrow-compression} module) at runtime.
   */
  public IpcOption withCompression(CompressionUtil.CodecType codecType) {
    return new IpcOption(write_legacy_ipc_format, metadataVersion, codecType, Optional.empty());
  }

  /** Like {@link #withCompression(CompressionUtil.CodecType)}, with a codec-specific level. */
  public IpcOption withCompression(CompressionUtil.CodecType codecType, int compressionLevel) {
    return new IpcOption(
        write_legacy_ipc_format, metadataVersion, codecType, Optional.of(compressionLevel));
  }
}
