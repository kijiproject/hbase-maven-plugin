// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A maven goal that stops the mini HBase cluster started by the 'start' goal.
 *
 * @goal stop
 * @phase post-integration-test
 */
public class StopMojo extends AbstractMojo {
  /** {@inheritDoc} */
  @Override
  public void execute() throws MojoExecutionException {
    MiniHBaseClusterSingleton.INSTANCE.stop(getLog());
  }
}
