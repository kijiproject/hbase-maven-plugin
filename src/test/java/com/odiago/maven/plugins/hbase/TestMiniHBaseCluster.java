/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
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

    replayMocks();
    MiniHBaseCluster cluster = new MiniHBaseCluster(mLog, true /* Enable MR */, mHBaseTestUtil);
    cluster.startup();
    cluster.shutdown();
    verifyMocks();
  }
}
