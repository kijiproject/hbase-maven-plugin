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
  private HBaseTestingUtility mTestUtil;
  /** Whether the cluster is running. */
  private boolean mIsRunning;

  /**
   * Creates a new <code>MiniHBaseCluster</code> instance.
   *
   * @param log The maven log.
   */
  public MiniHBaseCluster(Log log) {
    super(log);
    mTestUtil = new HBaseTestingUtility();
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
    mTestUtil.startMiniCluster();
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
    mTestUtil.shutdownMiniCluster();
  }
}
