// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A maven goal that starts a mini HBase cluster in a new daemon thread.
 *
 * @goal start
 * @phase pre-integration-test
 */
public class StartMojo extends AbstractMojo {
  /** {@inheritDoc} */
  @Override
  public void execute() throws MojoExecutionException {
    // TODO
  }
}
