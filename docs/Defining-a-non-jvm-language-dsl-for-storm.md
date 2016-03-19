---
layout: documentation
---
The right place to start to learn how to make a non-JVM DSL for Storm is [storm-core/src/storm.thrift](https://github.com/apache/incubator-storm/blob/master/storm-core/src/storm.thrift). Since Storm topologies are just Thrift structures, and Nimbus is a Thrift daemon, you can create and submit topologies in any language.

When you create the Thrift structs for spouts and bolts, the code for the spout or bolt is specified in the ComponentObject struct:

```
union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}
```

For a Python DSL, you would want to make use of "2" and "3". ShellComponent lets you specify a script to run that component (e.g., your python code). And JavaObject lets you specify native java spouts and bolts for the component (and Storm will use reflection to create that spout or bolt).

There's a "storm shell" command that will help with submitting a topology. Its usage is like this:

```
storm shell resources/ python topology.py arg1 arg2
```

storm shell will then package resources/ into a jar, upload the jar to Nimbus, and call your topology.py script like this:

```
python topology.py arg1 arg2 {nimbus-host} {nimbus-port} {uploaded-jar-location}
```

Then you can connect to Nimbus using the Thrift API and submit the topology, passing {uploaded-jar-location} into the submitTopology method. For reference, here's the submitTopology definition:

```java
void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
```

Finally, one of the key things to do in a non-JVM DSL is make it easy to define the entire topology in one file (the bolts, spouts, and the definition of the topology).
