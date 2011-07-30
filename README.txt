(c) Copyright 2011 Odiago, Inc.

hbase-maven-plugin : A maven plugin that starts/stops an HBase cluster.

This plugin is useful for integration testing code that interacts with an HBase
cluster.  Typically, you will bind the 'start' goal to your pre-integration-test
phase and the 'stop' goal to the post-integration-test phase of the maven
build lifecycle.

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
