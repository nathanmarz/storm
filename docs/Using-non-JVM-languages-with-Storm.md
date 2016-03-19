---
layout: documentation
---
- two pieces: creating topologies and implementing spouts and bolts in other languages
- creating topologies in another language is easy since topologies are just thrift structures (link to storm.thrift)
- implementing spouts and bolts in another language is called a "multilang components" or "shelling"
   - Here's a specification of the protocol: [Multilang protocol](Multilang-protocol.html)
   - the thrift structure lets you define multilang components explicitly as a program and a script (e.g., python and the file implementing your bolt)
   - In Java, you override ShellBolt or ShellSpout to create multilang components
       - note that output fields declarations happens in the thrift structure, so in Java you create multilang components like the following:
            - declare fields in java, processing code in the other language by specifying it in constructor of shellbolt
   - multilang uses json messages over stdin/stdout to communicate with the subprocess
   - storm comes with ruby, python, and fancy adapters that implement the protocol. show an example of python
      - python supports emitting, anchoring, acking, and logging
- "storm shell" command makes constructing jar and uploading to nimbus easy
  - makes jar and uploads it
  - calls your program with host/port of nimbus and the jarfile id

## Notes on implementing a DSL in a non-JVM language

The right place to start is src/storm.thrift. Since Storm topologies are just Thrift structures, and Nimbus is a Thrift daemon, you can create and submit topologies in any language.

When you create the Thrift structs for spouts and bolts, the code for the spout or bolt is specified in the ComponentObject struct:

```
union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}
```

For a non-JVM DSL, you would want to make use of "2" and "3". ShellComponent lets you specify a script to run that component (e.g., your python code). And JavaObject lets you specify native java spouts and bolts for the component (and Storm will use reflection to create that spout or bolt).

There's a "storm shell" command that will help with submitting a topology. Its usage is like this:

```
storm shell resources/ python topology.py arg1 arg2
```

storm shell will then package resources/ into a jar, upload the jar to Nimbus, and call your topology.py script like this:

```
python topology.py arg1 arg2 {nimbus-host} {nimbus-port} {uploaded-jar-location}
```

Then you can connect to Nimbus using the Thrift API and submit the topology, passing {uploaded-jar-location} into the submitTopology method. For reference, here's the submitTopology definition:

```
void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology)
    throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
```
