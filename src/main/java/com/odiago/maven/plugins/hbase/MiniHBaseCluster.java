// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
   */
  public MiniHBaseCluster(Log log, boolean enableMapReduce) {
    this(log, enableMapReduce, new HBaseTestingUtility());
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
    mTestUtil = hbaseTestUtil;
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

      mTestUtil.startMiniMapReduceCluster();
      getLog().info("MapReduce cluster started.");
    }
    mIsRunning = true;
  }

  /**
   * Stops the cluster.  Blocks until shut down.
   *
   * @throws IOException If there is an error.
   */
  public void shutdown() throws IOException {
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
}
