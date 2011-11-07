# Example Storm topologies

storm-starter contains a variety of examples of using Storm. If this is your first time checking out Storm, check out these topologies first:

1. ExclamationTopology: Basic topology written in all Java
2. WordCountTopology: Basic topology that makes use of multilang by implementing one bolt in Python
3. ReachTopology: Example of complex DRPC on top of Storm

More information about Storm can be found on the [project page](http://github.com/nathanmarz/storm).

## Running an example with Leiningen

```
lein deps
lein compile
java -cp `lein classpath` storm.starter.ExclamationTopology
```

## Maven

Maven is an alternative to Leiningen. storm-starter contains m2-pom.xml which can be used with Maven using the -f option, e.g.:

```
mvn -f m2-pom.xml package
```