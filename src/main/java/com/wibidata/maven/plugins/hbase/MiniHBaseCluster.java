/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.wibidata.maven.plugins.hbase;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.maven.plugin.logging.Log;

/**
 * A in-process mini HBase cluster that may be started and stopped.
 */
public class MiniHBaseCluster extends MavenLogged {

  /** An HBase testing utility for starting/stopping the cluster. */
  private final HBaseTestingUtility mTestUtil;

  /** Whether a mini MapReduce cluster should also be run. */
  private final boolean mIsMapReduceEnabled;

  /** Whether the cluster is running. */
  private boolean mIsRunning;

  /**
   * Creates a new <code>MiniHBaseCluster</code> instance.
   *
   * @param log The maven log.
   * @param enableMapReduce Whether to also use a mini MapReduce cluster.
   * @param conf Hadoop configuration for the cluster.
   */
  public MiniHBaseCluster(Log log, boolean enableMapReduce, Configuration conf) {
    this(log, enableMapReduce, new HBaseTestingUtility(conf));
  }

  /**
   * Creates a new <code>MiniHBaseCluster</code> instance.
   *
   * @param log The maven log.
   * @param enableMapReduce Whether to also use a mini MapReduce cluster.
   * @param hbaseTestUtil An HBase testing utility that can start and stop mini clusters.
   */
  public MiniHBaseCluster(Log log, boolean enableMapReduce, HBaseTestingUtility hbaseTestUtil) {
    super(log);
    mTestUtil = configure(hbaseTestUtil);
    mIsMapReduceEnabled = enableMapReduce;
    mIsRunning = false;
  }

  /**
   * Provides access to the HBase cluster configuration.
   *
   * @return The cluster configuration.
   */
  public Configuration getConfiguration() {
    return mTestUtil.getConfiguration();
  }

  /**
   * Determine whether the cluster is running.
   *
   * @return Whether the cluster is running.
   */
  public boolean isRunning() {
    return mIsRunning;
  }

  /**
   * Starts the cluster.  Blocks until ready.
   *
   * @throws Exception If there is an error.
   */
  public void startup() throws Exception {
    if (isRunning()) {
      throw new RuntimeException("Cluster already running.");
    }
    mTestUtil.startMiniCluster();
    if (mIsMapReduceEnabled) {
      getLog().info("Starting MapReduce cluster...");

      // Work around a bug in HBaseTestingUtility that requires this conf var to be set.
      getConfiguration().set("hadoop.log.dir", getConfiguration().get("hadoop.tmp.dir"));

      // Start a mini MapReduce cluster with one server.
      mTestUtil.startMiniMapReduceCluster(1);

      // Set the mapred.working.dir so stuff like partition files get written somewhere reasonable.
      getConfiguration().set("mapred.working.dir",
          mTestUtil.getDataTestDir("mapred-working").toString());

      getLog().info("MapReduce cluster started.");
    }
    mIsRunning = true;
  }

  /**
   * Stops the cluster.  Blocks until shut down.
   *
   * @throws Exception If there is an error.
   */
  public void shutdown() throws Exception {
    if (!mIsRunning) {
      getLog().error(
          "Attempting to shut down a cluster, but one was never started in this process.");
      return;
    }
    if (mIsMapReduceEnabled) {
      getLog().info("Shutting down MapReduce cluster...");
      mTestUtil.shutdownMiniMapReduceCluster();
      getLog().info("MapReduce cluster shut down.");
    }
    mTestUtil.shutdownMiniCluster();
  }

  /**
   * Configures an HBase testing utility.
   *
   * @param testUtil The test utility to configure.
   * @return The configured utility.
   */
  private static HBaseTestingUtility configure(HBaseTestingUtility testUtil) {
    // If HBase servers are running locally, the utility will use
    // the "normal" ports. We override *all* ports first, so that
    // we ensure that this can start without a problem.
    Configuration conf = testUtil.getConfiguration();

    int offset = new Random(System.currentTimeMillis()).nextInt(1500) + 500;

    // Move the master to a hopefully unused port.
    conf.setInt(HConstants.MASTER_PORT, findOpenPort(HConstants.DEFAULT_MASTER_PORT + offset));
    // Disable the master's web UI.
    conf.setInt("hbase.master.info.port", -1);

    // Move the regionserver to a hopefully unused port.
    conf.setInt(HConstants.REGIONSERVER_PORT,
        findOpenPort(HConstants.DEFAULT_REGIONSERVER_PORT + offset));
    // Disable the regionserver's web UI.
    conf.setInt("hbase.regionserver.info.port", -1);

    // Increase max zookeeper client connections.
    conf.setInt("hbase.zookeeper.property.maxClientCnxns", 80);

    // TODO(gwu): Increasing the port numbers by a constant is not sufficient for multiple
    // executions of this plugin on the same machine.  Allow this to be specified as a
    // maven plugin parameter.

    return testUtil;
  }

  /**
   * Find an available port.
   *
   * @param startPort the starting port to check.
   * @return an open port number.
   * @throws IllegalArgumentException if it can't find an open port.
   */
  public static int findOpenPort(int startPort) {
    if (startPort < 1024 || startPort > 65534) {
      throw new IllegalArgumentException("Invalid start port: " + startPort);
    }

    for (int port = startPort; port < 65534; port++) {
      ServerSocket ss = null;
      try {
        ss = new ServerSocket(port);
        ss.setReuseAddress(true);
        return port;
      } catch (IOException ioe) {
        // This port isn't open. Loop around.
      } finally {
        if (ss != null) {
          try {
            // TODO(aaron): Techincally, this causes a race condition. Another instance of
            // findOpenPort() could determine that this port is available between now and
            // when the client of this method calls this function.
            ss.close();
          } catch (IOException ioe) {
            // Shouldn't happen.
          }
        }
      }
    }

    throw new IllegalArgumentException("No port available starting at " + startPort);
  }
}
