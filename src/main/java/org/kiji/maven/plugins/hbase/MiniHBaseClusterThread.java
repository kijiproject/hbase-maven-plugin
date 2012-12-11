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

package org.kiji.maven.plugins.hbase;

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
        getLog().debug("Main thread interrupted while waiting for cluster to stop.");
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
