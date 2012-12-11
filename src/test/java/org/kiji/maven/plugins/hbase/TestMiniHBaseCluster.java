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

import java.net.ServerSocket;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

    // Expect the HBase cluster to be configured, started, and stopped.
    expect(mHBaseTestUtil.getConfiguration()).andReturn(new Configuration()).anyTimes();
    expect(mHBaseTestUtil.startMiniCluster()).andReturn(null);
    mHBaseTestUtil.shutdownMiniCluster();

    replayMocks();
    MiniHBaseCluster cluster = new MiniHBaseCluster(mLog, false /* Disable MR */, mHBaseTestUtil);
    cluster.startup();
    cluster.shutdown();
    verifyMocks();
  }

  @Test
  public void testWithMapReduceEnabled() throws Exception {
    // Expect any number of log calls.
    mLog.info(anyObject(String.class));
    expectLastCall().anyTimes();

    // Expect the HBase testing utility to be configured.
    expect(mHBaseTestUtil.getConfiguration()).andReturn(new Configuration()).anyTimes();

    // Expect the HBase cluster to be started and stopped.
    expect(mHBaseTestUtil.startMiniCluster()).andReturn(null);
    mHBaseTestUtil.shutdownMiniCluster();

    // Expect that the MapReduce cluster will be started and stopped.
    mHBaseTestUtil.startMiniMapReduceCluster(1);

    mHBaseTestUtil.shutdownMiniMapReduceCluster();
    // Expect the HBase testing utility to request a test dir for mapred.
    expect(mHBaseTestUtil.getDataTestDir(anyObject(String.class)))
        .andReturn(new Path("/mapred-working"));

    replayMocks();
    MiniHBaseCluster cluster = new MiniHBaseCluster(mLog, true /* Enable MR */, mHBaseTestUtil);
    cluster.startup();
    cluster.shutdown();
    verifyMocks();
  }

  @Test
  public void testListenerOccupied() throws Exception {
    // Test that findOpenPort() doesn't return a port we know to be in use.
    ServerSocket ss = new ServerSocket(9867);
    ss.setReuseAddress(true);
    try {
      int openPort = MiniHBaseCluster.findOpenPort(9867);
      assertTrue("Port 9867 is already bound!", openPort > 9867);
    } finally {
      ss.close();
    }
  }

  @Test
  public void testListenerOpen() throws Exception {
    // Test that findOpenPort() can return a port we don't believe is in use.
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(9867);
      ss.setReuseAddress(true);
    } finally {
      if (null != ss) {
        ss.close();
      }
    }

    // Port 9867 is unlikely to be in use, since we just successfully bound to it
    // and closed it.

    int openPort = MiniHBaseCluster.findOpenPort(9867);
    assertEquals("Port 9867 shouldn't be currently bound!", 9867, openPort);
  }
}
