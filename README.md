hbase-maven-plugin
==================

A maven plugin that starts/stops a mini HBase cluster.

This plugin is useful for integration testing code that interacts with
an HBase cluster.  Typically, you will bind the `start` goal to your
`pre-integration-test` phase and the `stop` goal to the
`post-integration-test` phase of the maven build lifecycle.


### Documentation

* [Plugin Usage](http://docs.kiji.org/apidocs/hbase-maven-plugin/1.0.9-cdh4/usage.html)
* [Plugin Goals](http://docs.kiji.org/apidocs/hbase-maven-plugin/1.0.9-cdh4/plugin-info.html)
* [Java API (Javadoc)](http://docs.kiji.org/apidocs/hbase-maven-plugin/1.0.9-cdh4/apidocs/)
