/**
 * Copyright (c) 2019. Qubole Inc
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

package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.bookkeeper.exception.WorkerInitializationException;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER_MULTINODE;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class);
  private LoadingCache<String, List<ClusterNode>> nodeStateMap;
  private HeartbeatService heartbeatService;
  private BookKeeperFactory bookKeeperFactory;
  private RetryingBookkeeperClient coordinatorClient;
  // The hostname of the master node.
  private String masterHostname;
  private int clusterType;
  RetryingBookkeeperClient client;

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws BookKeeperInitializationException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker(), new BookKeeperFactory());
  }

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, BookKeeperFactory factory)
      throws BookKeeperInitializationException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker(), factory);
  }

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker, BookKeeperFactory factory)
      throws BookKeeperInitializationException
  {
    super(conf, bookKeeperMetrics, Ticker.systemTicker());
    this.bookKeeperFactory = factory;
    this.masterHostname = ClusterUtil.getMasterHostname(conf);
    this.clusterType = CacheConfig.getClusterType(conf);

    initializeWorker(conf, metrics, ticker, factory);
  }

  private void initializeWorker(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory)
      throws WorkerInitializationException
  {
    startHeartbeatService(conf, metrics, factory);
    initializeNodeStateCache(conf, ticker);
    setCurrentNodeName();
  }

  void setCurrentNodeName() throws WorkerInitializationException
  {
    String nodeHostName;
    String nodeHostAddress;
    try {
      nodeHostName = InetAddress.getLocalHost().getCanonicalHostName();
      nodeHostAddress = InetAddress.getLocalHost().getHostAddress();
      log.info(" HostName : " + nodeHostName + " HostAddress : " + nodeHostAddress);
    }
    catch (UnknownHostException e) {
      log.warn("Could not get nodeName", e);
      throw new WorkerInitializationException("Could not get NodeName ", e);
    }

    List<ClusterNode> nodeList = getClusterNodes();

    if (nodeList != null && nodeList.size() > 0) {
      if (clusterType == TEST_CLUSTER_MANAGER.ordinal() || clusterType == TEST_CLUSTER_MANAGER_MULTINODE.ordinal()) {
        nodeName = nodeList.get(0).nodeUrl;
        return;
      }
      if (nodeList.indexOf(nodeHostName) >= 0) {
        nodeName = nodeHostName;
      }
      else if (nodeList.indexOf(nodeHostAddress) >= 0) {
        nodeName = nodeHostAddress;
      }
      else {
        String errorMessage = String.format("Could not find nodeHostName=%s nodeHostAddress=%s among cluster nodes=%s  " +
            "provide by master bookkeeper", nodeHostName, nodeHostAddress, nodeList);
        log.error(errorMessage);
        throw new WorkerInitializationException(errorMessage);
      }
    }
    else {
      String errorMessage = String.format("Could not find nodeHostName=%s nodeHostAddress=%s among cluster nodes=%s  " +
              "provide by master bookkeeper", nodeHostName, nodeHostAddress, nodeList);
      log.error(errorMessage);
      throw new WorkerInitializationException(errorMessage);
    }
  }

  private void initializeNodeStateCache(final Configuration conf, final Ticker ticker)
  {
    int expiryPeriod = CacheConfig.getWorkerNodeInfoExpiryPeriod(conf);
    nodeStateMap = CacheBuilder.newBuilder()
        .ticker(ticker)
        .refreshAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .build(new CacheLoader<String, List<ClusterNode>>() {
          @Override
          public List<ClusterNode> load(String s) throws Exception
          {
            if (client == null) {
              client = createBookKeeperClientWithRetry(bookKeeperFactory, masterHostname, conf);
            }
            return client.getClusterNodes();
          }
        });
  }

  @Override
  public void handleHeartbeat(String workerHostname, HeartbeatStatus request)
  {
    throw new UnsupportedOperationException("Worker node should not handle heartbeat");
  }

  @Override
  public List<ClusterNode> getClusterNodes()
  {
    try {
      return nodeStateMap.get("nodes");
    }
    catch (Exception e) {
      log.error("Could not get node host name from cache with Exception : " + e);
    }

    return null;
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
  {
    this.heartbeatService = new HeartbeatService(conf, metrics, factory, this);
    heartbeatService.startAsync();
  }

  /**
   * Attempt to initialize the client for communicating with the master BookKeeper.
   *
   * @param bookKeeperFactory   The factory to use for creating a BookKeeper client.
   * @return The client used for communication with the master node.
   */
  private static RetryingBookkeeperClient createBookKeeperClientWithRetry(BookKeeperFactory bookKeeperFactory,
                                                                          String hostName, Configuration conf)
  {
    final int retryInterval = CacheConfig.getServiceRetryInterval(conf);
    final int maxRetries = CacheConfig.getServiceMaxRetries(conf);

    return bookKeeperFactory.createBookKeeperClient(hostName, conf, maxRetries, retryInterval, false);
  }
}
