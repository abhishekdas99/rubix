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
package com.qubole.rubix.spi;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private long splitSize = 256 * 1024 * 1024; // 256MB

  private int nodeRefreshTime = 300; //sec

  public static String splitSizeConf = "caching.fs.split-size";

  public static String nodeRefreshTimeConf = "caching.fs.node-refresh-time";

  public ClusterType getClusterType()
  {
    return null;
  }

  public void initialize(Configuration conf)

  {
    splitSize = conf.getLong(splitSizeConf, splitSize);
    nodeRefreshTime = conf.getInt(nodeRefreshTimeConf, nodeRefreshTime);
  }

  public int getNodeIndex(int numNodes, String key)
  {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.hashString(key, Charsets.UTF_8);
    int nodeIndex = Hashing.consistentHash(hc, numNodes);
    return nodeIndex;
  }

  // This is the size in which the file will be logically divided into splits
  public long getSplitSize()
  {
    return splitSize;
  }

  public int getNodeRefreshTime()
  {
    return nodeRefreshTime;
  }

  public abstract boolean isMaster()
      throws ExecutionException;

  // Nodes format as per the note above
  // Should return sorted list
  public abstract List<String> getNodes();
}
