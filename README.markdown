# Example Storm topologies

storm-starter contains a variety of examples of using Storm. If this is your first time checking out Storm, check out these topologies first:

1. ExclamationTopology: Basic topology written in all Java
2. WordCountTopology: Basic topology that makes use of multilang by implementing one bolt in Python
3. ReachTopology: Example of complex DRPC on top of Storm

More information about Storm can be found on the [project page](http://github.com/nathanmarz/storm).

## Running an example with Leiningen

Install Leiningen by following the installation instructions [here](https://github.com/technomancy/leiningen).

### To run a Java example:

```
lein deps
lein compile
java -cp $(lein classpath) storm.starter.ExclamationTopology
```

### To run a Clojure example:

```
lein deps
lein compile
lein run -m storm.starter.clj.word-count
```

## Maven

Maven is an alternative to Leiningen. storm-starter contains m2-pom.xml which can be used with Maven using the -f option. For example, to compile and run `WordCountTopology` in local mode, use this command:

```
mvn -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.starter.WordCountTopology
```

You can package a jar suitable for submitting to a cluster with this command:

```
mvn -f m2-pom.xml package
```

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path `target/storm-starter-{version}-jar-with-dependencies.jar`.
