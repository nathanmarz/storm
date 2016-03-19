---
layout: documentation
---
To develop topologies, you'll need the Storm jars on your classpath. You should either include the unpacked jars in the classpath for your project or use Maven to include Storm as a development dependency. Storm is hosted on Clojars (a Maven repository). To include Storm in your project as a development dependency, add the following to your pom.xml:

```xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

```xml
<dependency>
  <groupId>storm</groupId>
  <artifactId>storm</artifactId>
  <version>0.7.2</version>
  <scope>test</scope>
</dependency>
```

[Here's an example](https://github.com/nathanmarz/storm-starter/blob/master/m2-pom.xml) of a pom.xml for a Storm project.

If Maven isn't your thing, check out [leiningen](https://github.com/technomancy/leiningen). Leiningen is a build tool for Clojure, but it can be used for pure Java projects as well. Leiningen makes builds and dependency management using Maven dead-simple. Here's an example project.clj for a pure-Java Storm project:

```clojure
(defproject storm-starter "0.0.1-SNAPSHOT"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :dependencies []
  :dev-dependencies [
                     [storm "0.7.2"]
                     ])
```

You can fetch dependencies using `lein deps`, build the project with `lein compile`, and make a jar suitable for submitting to a cluster with `lein uberjar`.

### Using Storm as a library

If you want to use Storm as a library (e.g., use the Distributed RPC client) and have the Storm dependency jars be distributed with your application, there's a separate Maven dependency called "storm/storm-lib". The only difference between this dependency and the usual "storm/storm" is that storm-lib does not have any logging configured.

### Developing Storm

You will want to

	bash ./bin/install_zmq.sh   # install the jzmq dependency
	lein sub install

Build javadocs with

	bash ./bin/javadoc.sh

### Building a Storm Release

Use the file `bin/build_release.sh` to make a zipfile like the ones you would download (and like what the bin files require in order to run daemons).
