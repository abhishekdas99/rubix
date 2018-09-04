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

package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.common.TestUtil;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCoordinatorBookKeeper
{
  private static final Log log = LogFactory.getLog(TestCoordinatorBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestCoordinatorBookKeeper");
  private static final String TEST_HOSTNAME_WORKER1 = "worker1";
  private static final String TEST_HOSTNAME_WORKER2 = "worker2";
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp()
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    metrics = new MetricRegistry();
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that the worker liveness count metric is correctly registered.
   */
  @Test
  public void testWorkerLivenessCountMetric() throws FileNotFoundException
  {
    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2);

    int workerCount = (int) metrics.getGauges().get(BookKeeperMetrics.LivenessMetric.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE.getMetricName()).getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");
  }

  /**
   * Verify that the worker liveness status properly expires.
   */
  @Test
  public void testWorkerLivenessCountMetric_workerLivenessExpired() throws FileNotFoundException
  {
    final FakeTicker ticker = new FakeTicker();
    final int workerLivenessExpiry = 1000; // ms
    CacheConfig.setWorkerLivenessExpiry(conf, workerLivenessExpiry);

    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics, ticker);
    final Gauge liveWorkerGauge = metrics.getGauges().get(BookKeeperMetrics.LivenessMetric.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE.getMetricName());

    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2);

    int workerCount = (int) liveWorkerGauge.getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");

    ticker.advance(workerLivenessExpiry, TimeUnit.MILLISECONDS);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);

    workerCount = (int) liveWorkerGauge.getValue();
    assertEquals(workerCount, 1, "Incorrect number of workers reporting heartbeat");
  }

  @Test
  public void testGetNodeHostNames() throws FileNotFoundException
  {
    CoordinatorBookKeeper coordinator = Mockito.spy(new CoordinatorBookKeeper(conf, metrics));
    try {
      Mockito.when(coordinator.getClusterManagerInstance(ClusterType.TEST_CLUSTER_MANAGER, conf)).thenReturn(
          new ClusterManager()
          {
            @Override
            public List<String> getNodes()
            {
              List<String> nodes = new ArrayList<String>();
              nodes.add("node1");
              nodes.add("node2");

              return nodes;
            }

            @Override
            public Integer getNextRunningNodeIndex(int startIndex)
            {
              return null;
            }

            @Override
            public Integer getPreviousRunningNodeIndex(int startIndex)
            {
              return null;
            }
          });
    }
    catch (ClusterManagerInitilizationException ex) {
      fail("Not able to initialize Cluster Manager");
    }

    List<String> hostNames = coordinator.getNodeHostNames(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    log.debug("HostNames : " + hostNames);
    assertTrue(hostNames.size() == 2, "Number of hosts does not match");
    assertTrue(hostNames.get(0).equals("node1") && hostNames.get(1).equals("node2"), "HostNames don't match");
  }
}
