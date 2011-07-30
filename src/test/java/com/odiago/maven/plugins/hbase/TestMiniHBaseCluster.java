// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import static org.easymock.EasyMock.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.maven.plugin.logging.Log;
import org.junit.Before;
import org.junit.Test;

public class TestMiniHBaseCluster {
  /** A mock maven log. */
  private Log mLog;

  /** A mock HBase testing utility that starts and stops clusters. */
  private HBaseTestingUtility mHBaseTestUtil;

  @Before
  public void createMocks() {
    mLog = createMock(Log.class);
    mHBaseTestUtil = createMock(HBaseTestingUtility.class);
  }

  /**
   * Tells the mock to start listening for expected calls.
   */
  private void replayMocks() {
    replay(mLog);
    replay(mHBaseTestUtil);
  }

  /**
   * Verifies that the mocks have received all their expected calls.
   */
  private void verifyMocks() {
    verify(mLog);
    verify(mHBaseTestUtil);
  }

  @Test
  public void testWithMapReduceDisabled() throws Exception {
    // Expect any number of log calls.
    mLog.info(anyObject(String.class));
    expectLastCall().anyTimes();

    // Expect the HBase cluster to be started and stopped.
    expect(mHBaseTestUtil.startMiniCluster()).andReturn(null);
    mHBaseTestUtil.shutdownMiniCluster();

    MiniHBaseCluster cluster = new MiniHBaseCluster(mLog, false /* Disable MR */, mHBaseTestUtil);

    replayMocks();
    cluster.startup();
    cluster.shutdown();
    verifyMocks();
  }

  @Test
  public void testWithMapReduceEnabled() throws Exception {
    // Expect any number of log calls.
    mLog.info(anyObject(String.class));
    expectLastCall().anyTimes();

    // Expect the HBase cluster to be started and stopped.
    expect(mHBaseTestUtil.startMiniCluster()).andReturn(null);
    mHBaseTestUtil.shutdownMiniCluster();

    // Expect that the MapReduce cluster will be started and stopped.
    expect(mHBaseTestUtil.getConfiguration()).andReturn(new Configuration()).anyTimes();
    mHBaseTestUtil.startMiniMapReduceCluster();
    mHBaseTestUtil.shutdownMiniMapReduceCluster();

    MiniHBaseCluster cluster = new MiniHBaseCluster(mLog, true /* Enable MR */, mHBaseTestUtil);

    replayMocks();
    cluster.startup();
    cluster.shutdown();
    verifyMocks();
  }
}
