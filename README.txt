(c) Copyright 2011 Odiago, Inc.

hbase-maven-plugin : A maven plugin that starts/stops an HBase cluster.

This plugin is useful for integration testing code that interacts with an HBase
cluster.  Typically, you will bind the 'start' goal to your pre-integration-test
phase and the 'stop' goal to the post-integration-test phase of the maven
build lifecycle.

--------------------------------------------------------------------------------

How to start an HBase cluster during the integration test phase:

To bind the goals to their default phases (pre- and post-integration-test), add
the following to the build plugins section of your pom.xml file:

  <build>
    <plugins>
      <!-- ... -->
      <plugin>
        <groupId>com.odiago.maven.plugins</groupId>
        <artifactId>hbase-maven-plugin</artifactId>
        <version>${version}</version>
        <executions>
          <execution>
            <goals>
              <goal>start</goal>
              <goal>stop</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <!-- ... -->
    </plugins>
  </build>

--------------------------------------------------------------------------------

How to additionally start a MapReduce cluster during the integration test phase:

You may also require a mini MapReduce cluster to be started alongside your HBase
cluster.  To configure the plugin to do so, set mapReduceEnabled to true in your
configuration:

  <plugin>
    <groupId>com.odiago.maven.plugins</groupId>
    <artifactId>hbase-maven-plugin</artifactId>
    <version>${version}</version>
    <configuration>
      <mapReduceEnabled>true</mapReduceEnabled>
    </configuration>
    <!-- ... -->
  </plugin>

--------------------------------------------------------------------------------

How to specify Hadoop configuration for your HBase and/or MapReduce cluster:

You might want to specify additional server-side Hadoop configuration for the
cluster.  You may use the <hadoopConfiguration> properties to set them.  For
example, to increase the number of map and reduce task slots:

  <plugin>
    <groupId>com.odiago.maven.plugins</groupId>
    <artifactId>hbase-maven-plugin</artifactId>
    <version>${version}</version>
    <configuration>
      <hadoopConfiguration>
        <property>
          <name>mapred.tasktracker.map.tasks.maximum</name>
          <value>16</value>
        </property>
        <property>
          <name>mapred.tasktracker.reduce.tasks.maximum</name>
          <value>8</value>
        </property>
      </hadoopConfiguration>
      <mapReduceEnabled>true</mapReduceEnabled>
    </configuration>
    <!-- ... -->
  </plugin>
