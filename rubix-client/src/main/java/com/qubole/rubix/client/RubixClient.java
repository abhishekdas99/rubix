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

import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Abhishek on 3/16/18.
 */
public class RubixClient
{
  private BookKeeperFactory factory;
  private Configuration conf;

  private static final Log log = LogFactory.getLog(RubixClient.class);

  public RubixClient(Configuration conf)
  {
    this.conf = conf;
    factory = new BookKeeperFactory();
  }

  public List<BlockLocation> getLocalCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock,
                                                 long endBlock, int clusterType)
  {
    return getCacheStatusInternal(null, remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
  }

  public List<BlockLocation> getRemoteCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock,
                                                  long endBlock, int clusterType, String host)
  {
    return getCacheStatusInternal(host, remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
  }

  private List<BlockLocation> getCacheStatusInternal(String host, String remotePath, long fileLength,
                                                     long lastModified, long startBlock, long endBlock,
                                                     int clusterType)
  {
    RetryingBookkeeperClient client = null;
    List<BlockLocation> result = new ArrayList<BlockLocation>();

    try {
      if (host != null) {
        client = factory.createBookKeeperClient(host, conf);
      }
      else {
        client = factory.createBookKeeperClient(conf);
      }

      result = client.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
    }
    catch (TTransportException ex) {
      log.error("Error while creating bookkeeper cleint");
    }
    catch (TException ex) {
      log.error("Error while invoking getCacheStatus");
    }
    finally {
      closeClient(client);
    }

    return result;
  }

  public boolean downloadDataInLocal(String remotePath, long offset, int length, long fileSize, long lastModified,
                                     int clusterType)
  {
    return downloadDataInternal(null, remotePath, offset, length, fileSize, lastModified, clusterType);
  }

  public boolean downloadDataInNonLocalNode(String remotePath, long offset, int length, long fileSize, long lastModified,
                                            int clusterType, String host)
  {
    return downloadDataInternal(host, remotePath, offset, length, fileSize, lastModified, clusterType);
  }

  private boolean downloadDataInternal(String host, String remotePath, long offset, int length,
                               long fileSize, long lastModified, int clusterType)
  {
    RetryingBookkeeperClient client = null;
    boolean dataDownloaded = false;

    try {
      if (host != null) {
        client = factory.createBookKeeperClient(host, conf);
      }
      else {
        client = factory.createBookKeeperClient(conf);
      }

      log.info("Downloading data from path : " + remotePath);
      dataDownloaded = client.readData(remotePath, offset, length, fileSize, lastModified, clusterType);
    }
    catch (TTransportException ex) {
      log.error("Error while creating bookkeeper cleint");
    }
    catch (TException ex) {
      log.error("Error while invoking readData " + ex.toString(), ex);
    }
    finally {
      closeClient(client);
    }

    return dataDownloaded;
  }

  public Map<String, Double> getCacheStats()
  {
    RetryingBookkeeperClient client = null;
    Map<String, Double> statsMap = new HashMap<String, Double>();

    try {
      client = factory.createBookKeeperClient(conf);
      statsMap = client.getCacheStats();
    }
    catch (Exception ex) {
      log.error("Error while invoking getCacheStats " + ex.toString(), ex);
    }
    finally {
      closeClient(client);
    }

    return statsMap;
  }

  private void closeClient(RetryingBookkeeperClient client)
  {
    try {
      if (client != null) {
        client.close();
      }
    }
    catch (Exception ex) {
      log.error("Not able to close BookKeeper client");
    }
  }
}
