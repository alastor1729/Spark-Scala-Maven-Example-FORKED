
Example Maven Project for Scala Spark 2 Application
===================================================

# Introduction

This archive contains an example Maven project for `Scala (version 2.13.16)` and `Apache Spark (version 3.5.5)` application.

<br>
<br>

# Details

The `pom.xml` contains example dependencies for : -

* Spark 
* SLF4J
* LOG4J (acts as logging implementation for SLF4J)
* [grizzled-slf4](https://alvinalexander.com/scala/scala-logging-grizzled-slf4j) a Scala specific wrapper for SLF4J.
* [typesafe](https://github.com/lightbend/config) for config.
* scalatest for testing.

<br>

Note that Scala itself is just listed as another dependency which means a global installation of Scala is not required.
The `pom.xml` builds an uber-jar containing all the dependencies by default (including Scala jars).

<br>

The pom also includes two exec goals: -

* `exec:exec@run-local` - run the code using local spark instance.
* `exec:exec@run-yarn`  - run the code on a remote yarn cluster. In order for this to work the `hive-site.xml`, `core-site.xml` and `yarn-site.xml` configuration files from the remote cluster must be copied into the `spark-remote/conf` directory.

<br>
<br>

# Note
To run this code from your IDE, the following exports must be added to the JVM args:-

```
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
```
This is also configured in the scalatest maven plugin in the pom: -

```xml
<argLine>--add-exports=java.base/sun.nio.ch=ALL-UNNAMED</argLine>
```

<br>
<br>

# Reference

[Maven for scala link](https://docs.scala-lang.org/tutorials/scala-with-maven.html)


<br>
<br>

# Reference to the original-Repo-Creator:

https://github.com/martinprobson/Spark-Scala-Maven-Example

<br>
<br>
