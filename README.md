hbase-maven-plugin
==================

A maven plugin that starts/stops a mini HBase cluster.

This plugin is useful for integration testing code that interacts with
an HBase cluster.  Typically, you will bind the `start` goal to your
`pre-integration-test` phase and the `stop` goal to the
`post-integration-test` phase of the maven build lifecycle.


### Documentation

* [Plugin Goals](http://wibidata.github.com/hbase-maven-plugin/1.0.3/plugin-info.html)
* [Java API (Javadoc)](http://wibidata.github.com/hbase-maven-plugin/1.0.3/apidocs/)
