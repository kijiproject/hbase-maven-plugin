// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import org.apache.maven.plugin.logging.Log;

/**
 * A thread that starts and runs a mini HBase cluster.
 */
public class MiniHBaseClusterThread extends Thread implements MavenLoggable {
  /** The maven log. */
  private final Log mLog;

  /** The local hbase cluster. */
  private final MiniHBaseCluster mHBaseCluster;

  /** Whether the cluster is started and ready. */
  private volatile boolean mIsClusterReady;

  /** Whether the thread has been asked to stop. */
  private volatile boolean mIsStopRequested;

  /**
   * Creates a new <code>MiniHBaseClusterThread</code> instance.
   *
   * @param log The maven log.
   * @param hbaseCluster The hbase cluster to run.
   */
  public MiniHBaseClusterThread(Log log, MiniHBaseCluster hbaseCluster) {
    mLog = log;
    mHBaseCluster = hbaseCluster;
    mIsClusterReady = false;
    mIsStopRequested = false;
  }

  /**
   * Determine whether the HBase cluster is up and running.
   *
   * @return Whether the cluster has completed startup.
   */
  public boolean isClusterReady() {
    return mIsClusterReady;
  }

  /**
   * Stops the HBase cluster gracefully.  When it is fully shut down, the thread will exit.
   */
  public void stopClusterGracefully() {
    mIsStopRequested = true;
    interrupt();
  }

  @Override
  public Log getLog() {
    return mLog;
  }

  /**
   * Runs the mini HBase cluster.
   *
   * <p>This method blocks until {@link #stopClusterGracefully()} is called.</p>
   */
  @Override
  public void run() {
    getLog().info("Starting up HBase cluster...");
    try {
      mHBaseCluster.startup();
    } catch (Exception e) {
      getLog().error("Unable to start an HBase cluster.", e);
      return;
    }
    getLog().info("HBase cluster started.");
    mIsClusterReady = true;
    yield();

    // Twiddle our thumbs until somebody requests the thread to stop.
    while (!mIsStopRequested) {
      try {
        sleep(1000);
      } catch (InterruptedException e) {
        // Whatever.
      }
    }

    getLog().info("Starting graceful shutdown of the HBase cluster...");
    try {
      mHBaseCluster.shutdown();
    } catch (Exception e) {
      getLog().error("Unable to stop the HBase cluster.", e);
      return;
    }
    getLog().info("HBase cluster shut down.");
  }
}
