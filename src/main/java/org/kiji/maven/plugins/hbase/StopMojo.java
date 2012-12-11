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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A maven goal that stops the mini HBase cluster started by the 'start' goal.
 *
 * @goal stop
 * @phase post-integration-test
 */
public class StopMojo extends AbstractMojo {

  /**
   * If true, this goal should be a no-op.
   *
   * @parameter property="skip" default-value="false"
   */
  private boolean mSkip;

  /**
   * The build directory for this project.
   *
   * @parameter property="projectBuildDir" default-value="${project.build.directory}"
   * @required
   */
  private String mProjectBuildDir;

  /**
   * If true, the Hadoop temporary directory (given by Hadoop configuration property hadoop.tmp
   * .dir) will be cleared before the cluster is started, then copied to the project's build
   * directory before the cluster is shutdown.
   *
   * @parameter property="saveHadoopTmpDir" expression="${save.hadoop.tmp}" default-value="false"
   * @required
   */
  private boolean mSaveHadoopTmpDir;

  /**
   * Sets the output directory for the project's build.
   *
   * @param dir The path to the output directory.
   */
  public void setProjectBuildDir(String dir) {
    mProjectBuildDir = dir;
  }

  /**
   * Sets whether this goal should be a no-op.
   *
   * @param skip If true, this goal should do nothing.
   */
  public void setSkip(boolean skip) {
    mSkip = skip;
  }

  /**
   * Sets whether the Hadoop temporary directory, given by hadoop.tmp.dir, should be cleared
   * before the cluster is started and copied to the project build directory before the cluster
   * is shutdown.
   *
   * @param saveTempDir If true, the directory will be copied to the project build directory
   *     before the cluster is shutdown.
   */
  public void setSaveHadoopTmpDir(boolean saveTempDir) {
    mSaveHadoopTmpDir = saveTempDir;
  }

  /**
   * Copies the directory indicated by hadoop.tmp.dir in the mini-cluster's configuration to the
   * project build directory.
   */
  private void copyHadoopTmpDir() {
    String tmpDirProperty =
        MiniHBaseClusterSingleton.INSTANCE.getClusterConfiguration().get("hadoop.tmp.dir");
    File hadoopTmp = new File(tmpDirProperty);
    File hadoopTmpCopy = new File(new File(mProjectBuildDir), "hadoop-tmp");
    getLog().info("Copying " + hadoopTmp.toString() + " to " + hadoopTmpCopy.toString());
    try {
      FileUtils.copyDirectory(hadoopTmp, hadoopTmpCopy);
      getLog().info("Successfully copied hadoop tmp dir.");
    } catch (IOException e) {
      getLog().warn("The Hadoop tmp dir could not be copied to the project's build directory.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void execute() throws MojoExecutionException {
    if (mSkip) {
      getLog().info("Not stopping an HBase cluster because skip=true.");
      return;
    }
    if (mSaveHadoopTmpDir) {
      copyHadoopTmpDir();
    }
    MiniHBaseClusterSingleton.INSTANCE.stop(getLog());
  }
}
