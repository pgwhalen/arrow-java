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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Keeps the compressed bodies of the messages a Flight client has sent alive for as long as the
 * corresponding uncompressed bodies would be.
 *
 * <p>A gRPC client call may serialize a message object after it was handed over and closed: it
 * queues messages until the call is connected, and replays them when it retries the call
 * transparently. An uncompressed body is a view of the writer's vectors, which keep it alive: the
 * record batch until the writer refills the root, and the dictionaries for the whole stream. A
 * compressed body is owned by the message alone, so this holder takes a reference to it in their
 * place:
 *
 * <ul>
 *   <li>a record batch's until the next record batch is sent, by which time the writer has refilled
 *       the root;
 *   <li>a dictionary batch's until the call ends.
 * </ul>
 *
 * <p>Everything is released when the call ends, after which gRPC no longer serializes messages.
 * Servers do not need this: a server call serializes each message as it is sent.
 */
final class CompressedBodyHolder {
  private final List<ArrowBuf> dictionaries = new ArrayList<>();
  private List<ArrowBuf> lastBatch = new ArrayList<>();
  private boolean callEnded = false;

  /** Hold the compressed body, if any, of a message that has just been sent. */
  synchronized void sent(ArrowMessage message) {
    final List<ArrowBuf> body = message.getCompressedBody();
    if (callEnded || body.isEmpty()) {
      return;
    }
    body.forEach(buf -> buf.getReferenceManager().retain());
    if (message.getMessageType() == ArrowMessage.HeaderType.DICTIONARY_BATCH) {
      dictionaries.addAll(body);
    } else {
      release(lastBatch);
      lastBatch = new ArrayList<>(body);
    }
  }

  /** Release everything held, as gRPC will no longer serialize the call's messages. */
  synchronized void callEnded() {
    callEnded = true;
    release(dictionaries);
    release(lastBatch);
  }

  private static void release(List<ArrowBuf> buffers) {
    buffers.forEach(buf -> buf.getReferenceManager().release());
    buffers.clear();
  }
}
