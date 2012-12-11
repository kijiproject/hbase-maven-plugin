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
   * @param alsoStartMapReduce Whether to also start a mini MapReduce cluster.
   * @param conf Hadoop configuration for the cluster.
   * @throws IOException If there is an error.
   */
  public void startAndWaitUntilReady(Log log, boolean alsoStartMapReduce, Configuration conf)
      throws IOException {
    mCluster = new MiniHBaseCluster(log, alsoStartMapReduce, conf);
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
   * Provides access to the cluster configuration after it has started.
   *
   * @return The configuration.
   */
  public Configuration getClusterConfiguration() {
    if (null == mCluster) {
      throw new IllegalStateException("The cluster has not started yet.");
    }
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
