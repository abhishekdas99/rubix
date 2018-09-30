/**
 * Copyright (c) 2018. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.qubole.rubix.core.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.DirectBufferPool;

import java.nio.ByteBuffer;

/**
 * Created by Abhishek on 9/29/18.
 */
public class BufferAllocator
{
  private static DirectBufferPool bufferPool;
  private static final Log log = LogFactory.getLog(BufferAllocator.class);

  private BufferAllocator()
  {
  }

  public static ByteBuffer allocateByteBuffer(int size)
  {
    if (bufferPool == null) {
      bufferPool = new DirectBufferPool();
    }

    ByteBuffer buffer = bufferPool.getBuffer(size);
    return buffer;
  }

  public static void releaseBuffer(ByteBuffer buffer)
  {
    if (bufferPool == null) {
      log.error("Trying to release a buffer that has not been initialized.");
      return;
    }

    bufferPool.returnBuffer(buffer);
  }
}
