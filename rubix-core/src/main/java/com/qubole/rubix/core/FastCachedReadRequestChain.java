/**
 * Copyright (c) 2016. Qubole Inc
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

package com.qubole.rubix.core;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by Abhishek on 1/11/18.
 */
public class FastCachedReadRequestChain extends ReadRequestChain
{
  private String fileToRead;
  private FileSystem.Statistics statistics = null;
  private Configuration conf;

  private static final Log log = LogFactory.getLog(FastCachedReadRequestChain.class);
  private static ListeningExecutorService cacheReadService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

  private static DirectBufferPool bufferPool = new DirectBufferPool();
  private int diskReadBufferSize;
  private int sizeRead = 0;

  public FastCachedReadRequestChain(String fileToRead, FileSystem.Statistics statistics, Configuration conf)
      throws IOException
  {
    this.fileToRead = fileToRead;
    this.statistics = statistics;
    this.conf = conf;
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSizeDefault(conf);
  }

  public Integer call() throws IOException
  {
    Thread.currentThread().setName(threadName);
    if (readRequests.size() == 0) {
      return 0;
    }

    checkState(isLocked, "Trying to execute Chain without locking");

    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }

      int toBeRead = readRequest.getActualReadLength();
      int perThreadRead = 200 * 1024;
      int numThreads = Math.min(10, Math.max(1, (toBeRead / perThreadRead)));
      int acutalPerThread = toBeRead / numThreads;
      log.info("Reading " + readRequest.getActualReadLength() + " data from position: " + readRequest.getActualReadStart() +
          "using " + numThreads + " threads with " + acutalPerThread + " data read by per thread");
      int allocatedRead = 0;

      ImmutableList.Builder builder = ImmutableList.builder();

      for (long start = readRequest.getActualReadStart(); start < readRequest.getActualReadEnd(); start += acutalPerThread) {
        int readLength = start + acutalPerThread > readRequest.getActualReadEnd() ?
            (int) (readRequest.getActualReadEnd() - start) : acutalPerThread;

        ReadCachedDataCallable callable = new ReadCachedDataCallable(fileToRead, conf, readRequest.getDestBuffer(), start,
            readLength, readRequest.getDestBufferOffset() + allocatedRead);
        allocatedRead += readLength;
        builder.add(cacheReadService.submit(callable));
      }

      List<ListenableFuture<Integer>> futures = builder.build();
      for (ListenableFuture<Integer> future : futures) {
        try {
          sizeRead += future.get();
          log.info("Read " + sizeRead + " till now");
        }
        catch (ExecutionException | InterruptedException e) {
          log.info("ERROR : " + Throwables.getStackTraceAsString(e));
        }
      }
    }

    return sizeRead;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setCachedDataRead(sizeRead).setCachedReads(requests);
  }

  private class ReadCachedDataCallable implements Callable<Integer>
  {
    Configuration conf;
    long readStart;
    int readLength;
    int bufferOffset;

    ByteBuffer directBuffer = null;
    FileChannel fileChannel = null;
    RandomAccessFile raf = null;
    byte[] destBuffer;

    public ReadCachedDataCallable(String fileToRead, Configuration conf, byte[] destBuffer,
                                  long readStart, int readLength, int bufferOffset) throws IOException
    {
      this.conf = conf;
      this.destBuffer = destBuffer;
      this.readStart = readStart;
      this.readLength = readLength;
      this.bufferOffset = bufferOffset;
      this.directBuffer = bufferPool.getBuffer(diskReadBufferSize);
      this.raf = new RandomAccessFile(fileToRead, "r");
      FileInputStream inputStream = new FileInputStream(raf.getFD());
      this.fileChannel = inputStream.getChannel();
    }

    public Integer call() throws IOException
    {
      log.info("Reading File : " + fileToRead + " From Start : " + readStart + " Length : " + readLength + " and " +
          " BufferOffset : " + bufferOffset);
      int nread = 0;
      int leftToRead = readLength;
      try {
        while (nread < readLength) {
          int readInThisCycle = Math.min(leftToRead, directBuffer.capacity());
          directBuffer.clear();
          int nbytes = fileChannel.read(directBuffer, readStart + nread);
          if (nbytes <= 0) {
            break;
          }
          directBuffer.flip();
          int transferBytes = Math.min(readInThisCycle, nbytes);
          directBuffer.get(destBuffer, bufferOffset + nread, transferBytes);
          leftToRead -= transferBytes;
          nread += transferBytes;
        }
      }
      finally {
        fileChannel.close();
        raf.close();
        bufferPool.returnBuffer(directBuffer);
        fileChannel = null;
        directBuffer = null;
        raf = null;
      }

      return nread;
    }
  }
}
