// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.maven.plugin.logging.Log;

/**
 * A singleton instance of a mini HBase cluster.
 */
public enum MiniHBaseClusterSingleton {
  /** The singleton instance. */
  INSTANCE;

  /** The thread that runs the mini HBase cluster. */
  private MiniHBaseClusterThread mThread;
  /** The HBase cluster being run. */
  private MiniHBaseCluster mCluster;

  /**
   * Starts the HBase cluster and blocks until it is ready.
   *
   * @param log The maven log.
   * @throws IOException If there is an error.
   */
  public void startAndWaitUntilReady(Log log) throws IOException {
    mCluster = new MiniHBaseCluster(log);
    mThread = new MiniHBaseClusterThread(log, mCluster);

    log.info("Starting new thread...");
    mThread.start();

    // Wait for the cluster to be ready.
    log.info("Waiting for cluster to be ready...");
    while (!mThread.isClusterReady()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.info("Still waiting...");
      }
    }
    log.info("Finished waiting for HBase cluster thread.");
  }

  /**
   * Provides access to the cluster configuration.
   *
   * @return The configuration.
   */
  public Configuration getClusterConfiguration() {
    return mCluster.getConfiguration();
  }

  /**
   * Stops the HBase cluster and blocks until is has been shutdown completely.
   *
   * @param log The maven log.
   */
  public void stop(Log log) {
    if (null == mCluster) {
      log.error("Attempted to stop a cluster, but no cluster was ever started in this process.");
      return;
    }

    log.info("Stopping the HBase cluster thread...");
    mThread.stopClusterGracefully();
    while (mThread.isAlive()) {
      try {
        mThread.join();
      } catch (InterruptedException e) {
        log.debug("HBase cluster thread interrupted.");
      }
    }
    log.info("HBase cluster thread stopped.");
  }
}
