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
package com.qubole.rubix.hadoop2;

import com.qubole.rubix.spi.ClusterManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterManager
{
  private Log log = LogFactory.getLog(TestClusterManager.class);

  // The REST API endpoint used for fetching node information from the cluster.
  private static final String CLUSTER_NODES_ENDPOINT = "/ws/v1/cluster/nodes";

  // The address used for testing the cluster manager.
  private static final String HADOOP2_CLUSTER_ADDRESS = "localhost:45326";

  // The worker hostnames used for verifying cluster manager behaviour
  private static final String WORKER_HOSTNAME_1 = "192.168.1.3";
  private static final String WORKER_HOSTNAME_2 = "192.168.2.252";
  private static final String WORKER_HOSTNAME_3 = "192.168.1.6";
  private static final String WORKER_HOSTNAME_4 = "192.168.2.4";
  private static final String WORKER_HOSTNAME_5 = "192.168.2.254";
  private static final String WORKER_HOSTNAME_6 = "192.168.2.255";

  @Test
  /*
   * Tests that the worker nodes returned are correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_multipleWorkers()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleRunningWorkers());

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2) && nodeHostnames.get(1).equals(WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that the single worker node returned is correctly handled by Hadoop2ClusterManager.
   */
  public void testGetNodes_oneWorker()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new OneRunningWorker());

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_1));
  }

  @Test
  /*
   * Tests that the new worker nodes returned is correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_oneNewWorker()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneNew());

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2) && nodeHostnames.get(1).equals(WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that the rebooted worker node returned is correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_oneRebootedWorker()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneRebooted());

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2) && nodeHostnames.get(1).equals(WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a single node cluster, master node is returned as worker.
   */
  public void testMasterOnlyCluster()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new NoWorkers());

    assertTrue(nodeHostnames.size() == 1, "Should have added localhost in list");
    assertTrue(nodeHostnames.get(0).equals(InetAddress.getLocalHost().getHostAddress()), "Not added right hostname");
  }

  @Test
  /*
   * Tests that in a cluster with decommissioned node, decommissioned node is not returned.
   */
  public void testUnhealthyNodeCluster_decommissioned()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneDecommissioned());

    assertTrue(nodeHostnames.size() == 2, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with decommissioning node, decommissioning node is not returned.
   */
  public void testUnhealthyNodeCluster_decommissioning()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneDecommissioning());

    assertTrue(nodeHostnames.size() == 2, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with lost node, lost node is not returned.
   */
  public void testUnhealthyNodeCluster_lost()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneLost());

    assertTrue(nodeHostnames.size() == 2, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with unhealthy node, unhealthy node is not returned.
   */
  public void testUnhealthyNodeCluster_unhealthy()
      throws IOException
  {
    final List<String> nodeHostnames = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new MultipleWorkersOneUnhealthy());

    assertTrue(nodeHostnames.size() == 2, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  public void consistent_hashing_spotloss()
      throws IOException
  {
    String key = "1";
    final List<String> nodeHostnames1 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new FourWorkers());
    final List<String> nodeHostnames2 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, new FourLiveWorkersOneDecommissioned());

    assertTrue(nodeHostnames1.size() == 4, "Should have four nodes");
    assertTrue(nodeHostnames2.size() == 5, "Should have five nodes");

    int nodeIndex1 = getConsistentHashedNodeIndexFromCluster(CLUSTER_NODES_ENDPOINT, new FourWorkers(), key);
    int nodeIndex2 = getConsistentHashedNodeIndexFromCluster(CLUSTER_NODES_ENDPOINT, new FourLiveWorkersOneDecommissioned(), key);

    String nodeName1 = nodeHostnames1.get(nodeIndex1);
    String nodeName2 = nodeHostnames2.get(nodeIndex2);

    assertTrue(nodeName1.equals(nodeName2), "Both should be the same node");
  }

  @Test
  public void consistent_hashing_upscaling()
      throws IOException
  {
    int numKeys = 200;
    Set<String> keys = generateRandomKeys(numKeys);
    int match = matchMemberships(new FourWorkers(), new SixWorkers(), keys);
    int percenrage = ((match * 100) / numKeys);
    System.out.println(" Upscaling -- Membership remained the same for " + percenrage + "% of keys");
  }

  @Test
  public void consistent_hashing_downScaling()
      throws IOException
  {
    int numKeys = 200;
    Set<String> keys = generateRandomKeys(numKeys);
    int match = matchMemberships(new SixWorkers(), new FourLiveWorkersTwoDecommissioned(), keys);
    Map<String, Integer> keyMembership = getConsistentHashedMembership(new SixWorkers(), keys);
    int expected = 0;
    for (Map.Entry<String, Integer> entry : keyMembership.entrySet()) {
      if (entry.getValue() != 0 && entry.getValue() != 5) {
        expected++;
      }
    }

    assertTrue(match == expected, "Distribution of the keys didn't match");
    System.out.println(" Downscaling -- Membership changed only for the downscaled nodes ");
  }

  @Test
  public void consistent_hashing_spotloss2()
      throws IOException
  {
    int numKeys = 200;

    Set<String> keys = generateRandomKeys(numKeys);
    TestWorker prevWorker = new FourWorkers();

    final List<String> nodeHostnames1 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, prevWorker);
    Map<String, Integer> keyMembership1 = getConsistentHashedMembership(prevWorker, keys);

    TestWorker newWorker = new FourLiveWorkersOneDecommissioned();
    final List<String> nodeHostnames2 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, newWorker);
    Map<String, Integer> keyMembership2 = getConsistentHashedMembership(newWorker, keys);

    int reshuffle = 0;
    int match = 0;

    for (String key : keys) {
      if (nodeHostnames1.get(keyMembership1.get(key)).equals(nodeHostnames2.get(keyMembership2.get(key)))) {
        match++;
      }
      else if (keyMembership1.get(key) != 0) {
        reshuffle++;
      }
    }

    int percenrage = ((match * 100) / numKeys);
    System.out.println(" Spotloss -- Membership remained the same for " + percenrage + "% of keys");

    percenrage = ((reshuffle * 100) / numKeys);
    System.out.println(" Spotloss -- Membership reshuffled from lost node for " + percenrage + "% of the total keys ");
  }

  private int matchMemberships(TestWorker prevWorker, TestWorker newWorker, Set<String> keys)
      throws IOException
  {
    final List<String> nodeHostnames1 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, prevWorker);
    Map<String, Integer> keyMembership1 = getConsistentHashedMembership(prevWorker, keys);

    final List<String> nodeHostnames2 = getNodeHostnamesFromCluster(CLUSTER_NODES_ENDPOINT, newWorker);
    Map<String, Integer> keyMembership2 = getConsistentHashedMembership(newWorker, keys);

    int match = 0;
    int nonMatch = 0;

    for (String key : keys) {
      if (nodeHostnames1.get(keyMembership1.get(key)).equals(nodeHostnames2.get(keyMembership2.get(key)))) {
        match++;
      }
      else {
        nonMatch++;
      }
    }

    return match;
  }

  private Map<String, Integer> getConsistentHashedMembership(TestWorker worker, Set<String> keys)
      throws IOException
  {
    Map<String, Integer> keyMembership1 = new HashMap<>();
    int nodeIndex1 = 0;

    for (String key : keys) {
      nodeIndex1 = getConsistentHashedNodeIndexFromCluster(CLUSTER_NODES_ENDPOINT, worker, key);
      keyMembership1.put(key, nodeIndex1);
    }
    return keyMembership1;
  }

  private Set<String> generateRandomKeys(int numKeys)
  {
    Set<String> keys = new HashSet<String>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(getSaltString());
    }

    return keys;
  }

  protected String getSaltString()
  {
    String saltchars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    StringBuilder salt = new StringBuilder();
    Random rnd = new Random();
    while (salt.length() < 18) { // length of the random string.
      int index = (int) (rnd.nextFloat() * saltchars.length());
      salt.append(saltchars.charAt(index));
    }
    String saltStr = salt.toString();
    return saltStr;
  }

  /**
   * Initializes a {@link Hadoop2ClusterManager} for testing.
   *
   * @return The cluster manager instance to be tested.
   */
  private ClusterManager buildHadoop2ClusterManager()
  {
    final ClusterManager clusterManager = new Hadoop2ClusterManager();
    final Configuration conf = new Configuration();
    conf.set(Hadoop2ClusterManager.addressConf, HADOOP2_CLUSTER_ADDRESS);
    clusterManager.initialize(conf);
    return clusterManager;
  }

  /**
   * Create a server to mock a Hadoop endpoint.
   *
   * @param endpoint    The API endpoint to mock.
   * @param handler     The handler used to handle requests.
   * @return The mocked endpoint server.
   * @throws IOException if the server could not be created.
   */
  private HttpServer createServer(String endpoint, HttpHandler handler)
      throws IOException
  {
    final HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    server.createContext(endpoint, handler);
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  /**
   * Fetch a list of node hostnames from the specified endpoint.
   *
   * @param endpoint          The endpoint to query.
   * @param responseHandler   The handler used to return the desired response.
   * @return A list of hostnames for the nodes in the cluster.
   * @throws IOException if the cluster server could not be created.
   */
  private List<String> getNodeHostnamesFromCluster(String endpoint, HttpHandler responseHandler) throws IOException
  {
    final HttpServer server = createServer(endpoint, responseHandler);
    log.info("STARTED SERVER");

    final ClusterManager clusterManager = buildHadoop2ClusterManager();
    final List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    server.stop(0);
    return nodes;
  }

  private int getConsistentHashedNodeIndexFromCluster(String endpoint, HttpHandler responseHandler, String key)
      throws IOException
  {
    final HttpServer server = createServer(endpoint, responseHandler);
    log.info("STARTED SERVER");

    final ClusterManager clusterManager = buildHadoop2ClusterManager();
    final List<String> nodes = clusterManager.getNodes();
    final int index = clusterManager.getNodeIndex(nodes.size(), key);

    server.stop(0);
    return index;
  }

  /**
   * Http response handler base class.
   */
  private class TestWorker implements HttpHandler
  {
    private String nodeResponse;

    public TestWorker(String nodeJson)
    {
      this.nodeResponse = nodeJson;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException
    {
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodeResponse.length());
      final OutputStream os = exchange.getResponseBody();
      os.write(nodeResponse.getBytes());
      os.close();
    }
  }

  /**
   * Http response handler to represent a cluster with multiple running worker nodes.
   */
  private class MultipleRunningWorkers extends TestWorker
  {
    public MultipleRunningWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }
  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  private class MultipleWorkersOneDecommissioned extends TestWorker
  {
    public MultipleWorkersOneDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  private class MultipleWorkersOneDecommissioning extends TestWorker
  {
    public MultipleWorkersOneDecommissioning()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONING\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  private class MultipleWorkersOneLost extends TestWorker
  {
    public MultipleWorkersOneLost()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"LOST\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  private class MultipleWorkersOneNew extends TestWorker
  {
    public MultipleWorkersOneNew()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"NEW\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one rebooted worker node.
   */
  private class MultipleWorkersOneRebooted extends TestWorker
  {
    public MultipleWorkersOneRebooted()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"REBOOTED\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one unhealthy worker node.
   */
  private class MultipleWorkersOneUnhealthy extends TestWorker
  {
    public MultipleWorkersOneUnhealthy()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"UNHEALTHY\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with no worker nodes.
   */
  private class NoWorkers extends TestWorker
  {
    public NoWorkers()
    {
      super("{\"nodes\":{\"node\":[]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one running worker node.
   */
  private class OneRunningWorker extends TestWorker
  {
    public OneRunningWorker()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}]}}");
    }
  }

  private class FourWorkers extends TestWorker
  {
    public FourWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  private class FourLiveWorkersOneDecommissioned extends TestWorker
  {
    public FourLiveWorkersOneDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  private class SixWorkers extends TestWorker
  {
    public SixWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_6 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  private class FourLiveWorkersTwoDecommissioned extends TestWorker
  {
    public FourLiveWorkersTwoDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}, {\"nodeHostName\":\"" + WORKER_HOSTNAME_6 + "\",\"state\":\"DECOMMISSIONED\"}]}}\n");
    }
  }
}
