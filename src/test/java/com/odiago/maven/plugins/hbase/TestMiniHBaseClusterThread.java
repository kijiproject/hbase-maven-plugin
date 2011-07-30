// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import org.apache.maven.plugin.logging.Log;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMiniHBaseClusterThread {
  private static final Logger LOG = LoggerFactory.getLogger(TestMiniHBaseClusterThread.class);

  /**
   * Tests that the thread can be started up and shut down gracefully.
   */
  @Test
  public void testThreadStartupAndShutdown() throws Exception {
    // Create mocks.
    Log log = createMock(Log.class);
    MiniHBaseCluster hbaseCluster = createMock(MiniHBaseCluster.class);

    // Expect a bunch of log calls.
    log.info(anyObject(String.class));
    expectLastCall().anyTimes();
    log.debug(anyObject(String.class));
    expectLastCall().anyTimes();

    // Expect the cluster to be started and shut down.
    hbaseCluster.startup();
    hbaseCluster.shutdown();

    replay(log);
    replay(hbaseCluster);

    // Create the thread.
    MiniHBaseClusterThread thread = new MiniHBaseClusterThread(log, hbaseCluster);
    assertFalse(thread.isClusterReady());

    // Start the thread.
    thread.start();

    // Verify that the thread is running.
    assertTrue(thread.isAlive());

    // Wait for the cluster to be ready.
    while (!thread.isClusterReady()) {
      LOG.info("Cluster isn't ready yet...");
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.info("Thread interrupted. Continuing anyway...");
      }
    }
    LOG.info("Cluster is ready.");

    // Stop the thread.
    thread.stopClusterGracefully();
    LOG.info("Waiting for the thread to finish...");
    while (thread.isAlive()) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.info("Thread interrupted. Continuing anyway...");
      }
    }
    LOG.info("Thread finished.");

    verify(log);
    verify(hbaseCluster);
  }
}
