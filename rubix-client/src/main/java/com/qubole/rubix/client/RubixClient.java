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

import java.util.ArrayList;
import java.util.List;

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

  public List<BlockLocation> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock,
                                            long endBlock, int clusterType)
  {
    RetryingBookkeeperClient client = null;
    List<BlockLocation> result = new ArrayList<BlockLocation>();
    try {
      client = factory.createBookKeeperClient(conf);
      result = client.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
    }
    catch (Exception ex) {
      log.error("Error while invoking getCacheStatus");
    }
    finally {
      try {
        if (client != null) {
          client.close();
        }
      }
      catch (Exception ex) {
        log.error("Not able to close BookKeeper client");
      }
    }

    return result;
  }

  public boolean downloadData(String remotePath, long offset, int length, long fileSize, long lastModified,
                              int clusterType)
  {
    RetryingBookkeeperClient client = null;
    boolean dataDownloaded = false;
    try {
      client = factory.createBookKeeperClient(conf);
      log.info("Downloading data from path : " + remotePath);
      dataDownloaded = client.readData(remotePath, offset, length, fileSize, lastModified, clusterType);
    }
    catch (Exception ex) {
      log.error("Error while invoking readData " + ex.toString(), ex);
    }
    finally {
      try {
        if (client != null) {
          client.close();
        }
      }
      catch (Exception ex) {
        log.error("Not able to close BookKeeper client");
      }
    }

    return dataDownloaded;
  }
}
