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

package com.qubole.rubix.client;

import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Abhishek on 3/16/18.
 */
public class StressTest extends Configured implements Tool
{
  private static RubixClient client;
  private static Configuration conf;
  private ExecutorService processService;
  private static final String remoteFileName = "remoteFile";
  private static final int blockSize = 100;

  private int numThreads;
  private String remotePathForFile;

  private static final Log log = LogFactory.getLog(RubixClient.class);

  public StressTest()
  {
  }

  public static void main(String[] args) throws Exception
  {
    ToolRunner.run(new Configuration(), new StressTest(), args);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    try {
      conf = this.getConf();
      numThreads = conf.getInt("numThreads", 10);
      remotePathForFile = conf.get("remoteFilePath", "/media/ephemeral0/testData/");

      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
      processService = MoreExecutors.getExitingExecutorService(executor);
      CacheConfig.setBlockSize(conf, blockSize);

      try {
        Files.createDirectories(Paths.get(remotePathForFile));
        generateData();
      }
      catch (IOException ex) {
        log.error("Not able to create directory : " + remotePathForFile, ex);
      }

      client = new RubixClient(conf);
      performStressTests(client);
    }
    finally {
      HashMap<Integer, String> dirs = CacheUtil.getCacheDiskPathsMap(conf);
      for (String dir : dirs.values()) {
        Files.walkFileTree(Paths.get(dir + "/fcache"), new DeleteFileVisitor());
      }
    }
    return 0;
  }

  private void generateData() throws IOException
  {
    for (int i = 0; i < numThreads; i++) {
      String remoteFile = remotePathForFile + remoteFileName + i;
      log.info(remoteFile);
      StressTestUtils.populateFile(remoteFile);
    }
  }

  private void performStressTests(RubixClient client)
  {
    int res = stressTestDownloadData(client);
    log.info("Got Success in DownloadData " + res + " out of " + numThreads);
    res = stressTestDownloadDataForSameFile(client);
    res = stressTestGetCacheStatus(client);
    log.info("Got Success in GetCacheStatus " + res + " out of " + numThreads);
  }

  private int stressTestDownloadDataForSameFile(final RubixClient client)
  {
    String expectedOutput = StressTestUtils.generateContent().substring(0, 200);
    final String remoteFile = remotePathForFile + remoteFileName + (numThreads + 1);
    final Path remotePath = new Path("file:///" + remoteFile);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

    for (int i = 0; i < numThreads; i++) {
      Future<Boolean> future = processService.submit(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return client.downloadDataInLocal(remotePath.toString(), 0, 200, 10000, 10000, 3);
            }
          });
      futures.add(future);
    }

    int i = 0;
    int success = 0;

    for (Future<Boolean> future : futures) {
      try {
        boolean res = future.get();
        if (res == true) {
          if (validateContent(remotePath.toString(), 0, 200, expectedOutput)) {
            success++;
          }
        }
      }
      catch (ExecutionException | InterruptedException ex) {
        log.error("Problem in getting result. Exception : " + ex.toString(), ex);
      }
      i++;
    }

    return success;
  }

  private int stressTestDownloadData(final RubixClient client)
  {
    String expectedOutput = StressTestUtils.generateContent().substring(0, 200);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < numThreads; i++) {
      final String remoteFile = remotePathForFile + remoteFileName + i;
      final Path remotePath = new Path("file:///" + remoteFile);
      Future<Boolean> future = processService.submit(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return client.downloadDataInLocal(remotePath.toString(), 0, 200, 10000, 10000, 3);
            }
          });
      futures.add(future);
    }

    int i = 0;
    int success = 0;

    for (Future<Boolean> future : futures) {
      String remoteFile = remotePathForFile + remoteFileName + i;
      Path remotePath = new Path("file:///" + remoteFile);
      try {
        boolean res = future.get();
        if (res == true) {
          if (validateContent(remotePath.toString(), 0, 200, expectedOutput)) {
            success++;
          }
        }
      }
      catch (ExecutionException | InterruptedException ex) {
        log.error("Problem in getting result. Exception : " + ex.toString(), ex);
      }
      i++;
    }

    return success;
  }

  private boolean validateContent(String remotePath, int offset, int length, String expectedData)
  {
    String localFilePath = CacheUtil.getLocalPath(remotePath, conf);
    String result = "";
    try {
      byte[] cachedData = StressTestUtils.readBytesFromFile(localFilePath, offset, length);
      result = new String(cachedData);
    }
    catch (IOException ex) {
      log.error("Problem in reading file " + localFilePath);
    }

    return result.equals(expectedData);
  }

  private int stressTestGetCacheStatus(final RubixClient client)
  {
    List<Future<List<BlockLocation>>> futures = new ArrayList<Future<List<BlockLocation>>>();
    for (int i = 0; i < numThreads; i++) {
      final String remoteFile = remotePathForFile + remoteFileName + i;
      final Path remotePath = new Path("file:///" + remoteFile);
      Future<List<BlockLocation>> future = processService.submit(
          new Callable<List<BlockLocation>>()
          {
            @Override
            public List<BlockLocation> call() throws Exception
            {
              return client.getLocalCacheStatus(remotePath.toString(), 10000, 10000, 0, 1, 3);
            }
          });
      futures.add(future);
    }

    int i = 0;
    int success = 0;

    for (Future<List<BlockLocation>> future : futures) {
      String remoteFile = remotePathForFile + remoteFileName + i;
      Path remotePath = new Path("file:///" + remoteFile);
      try {
        List<BlockLocation> res = future.get();
        log.info("size -- " + res.size());
        if (res.get(0).getLocation() == Location.CACHED) {
          success++;
        }
      }
      catch (ExecutionException | InterruptedException ex) {
        log.error("Problem in getting result. Exception : " + ex.toString(), ex);
      }
      i++;
    }

    return success;
  }

  static class StressTestUtils
  {
    private StressTestUtils()
    {
    }

    static String generateContent(int jump)
    {
      StringBuilder stringBuilder = new StringBuilder();
      for (char i = 'a'; i <= 'z'; i = (char) (i + jump)) {
        for (int j = 0; j < 100; j++) {
          stringBuilder.append(i);
        }
      }
      return stringBuilder.toString();
    }

    static String generateContent()
    {
      return generateContent(1);
    }

    static String getExpectedOutput(int size)
    {
      String expected = generateContent(2);
      return expected.substring(0, size);
    }

    static void populateFile(String filename) throws IOException
    {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
      out.print(generateContent());
      out.close();
    }

    static byte[] readBytesFromFile(String path, int offset, int length) throws IOException
    {
      RandomAccessFile raf = new RandomAccessFile(path, "r");
      FileInputStream fis = new FileInputStream(raf.getFD());
      DirectBufferPool bufferPool = new DirectBufferPool();
      ByteBuffer directBuffer = bufferPool.getBuffer(2000);
      byte[] result = new byte[length];

      FileChannel fileChannel = fis.getChannel();

      int nread = 0;
      int leftToRead = length;

      while (nread < length) {
        int readInThisCycle = Math.min(leftToRead, directBuffer.capacity());
        directBuffer.clear();
        int nbytes = fileChannel.read(directBuffer, offset + nread);
        if (nbytes <= 0) {
          break;
        }
        directBuffer.flip();
        int transferBytes = Math.min(readInThisCycle, nbytes);
        directBuffer.get(result, nread, transferBytes);
        leftToRead -= transferBytes;
        nread += transferBytes;
      }

      return result;
    }
  }

  class DeleteFileVisitor extends SimpleFileVisitor<java.nio.file.Path>
  {
    @Override
    public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException
    {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc) throws IOException
    {
      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }
}
