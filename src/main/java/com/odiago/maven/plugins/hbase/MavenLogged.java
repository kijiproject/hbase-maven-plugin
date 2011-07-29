// (c) Copyright 2011 Odiago, Inc.

package com.odiago.maven.plugins.hbase;

import org.apache.maven.plugin.logging.Log;

/**
 * An abstract base class for objects that write to the maven log.
 */
public abstract class MavenLogged implements MavenLoggable {
  /** The maven log used to communicate with the maven user. */
  private final Log mLog;

  /**
   * Constructor.
   *
   * @param log The maven log.
   */
  protected MavenLogged(Log log) {
    mLog = log;
  }

  /** {@inheritDoc} */
  @Override
  public Log getLog() {
    return mLog;
  }
}
