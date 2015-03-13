# [flux] (http://en.wikipedia.org/wiki/Flux)

## About
What is "flux"?

Flux is a framework and set of utilities that make defining and deploying Apache Storm topologies less painful and
deveoper-intensive.

Have you ever found yourself repeating this pattern?:

```java

public static void main(String[] args) throws Exception {
    // logic to determine if we're running locally or not...
    // create necessary config options...

    boolean runLocal = shouldRunLocal();
    if(runLocal){
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, topology);
    } else {
        StormSubmitter.submitTopology(name, conf, topology);
    }
}
```

Wouldn't something like this be easier:

```bash
storm jar mytopology.jar --local config.yaml
```

or:

```bash
storm jar mytopology.jar --remote config.yaml
```


## Disclaimer

This is an experimental project that is rapidly changing. Documentation may be out of date. Code is the ultimate source
of truth.


## Usage

`storm jar flux-xxx.jar <config.yaml>`

```
usage: storm jar <my_topology_uber_jar.jar> [options]
             <topology-config.yaml>
 -l,--local           Run the topology in local mode.
 -r,--remote          Deploy the topology to a remote cluster.
 -R,--resource        Treat the supplied path as a classpath resource
                      instead of a file. (not implemented)
 -s,--sleep <sleep>   When running locally, the amount of time to sleep
                      (in ms.) before killing the topology and shutting
                      down the local cluster.
```

## YAML Configuration
Flux topologies are defined in a YAML file that describes a topology. A Flux topology
definition consists of the following:

  1. A topology name
  2. A list of topology "components" (named Java objects that will be made available in the environment)
  3. A list of spouts, each identified by a unique ID
  4. A list of bolts, each identified by a unique ID
  5. A list of "stream" objects representing a flow of tuples between spouts and bolts

For reference, a simple example of a wordcount topology is listed below:

```yaml
name: "yaml-topology"
config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "spout-1"
    className: "backtype.storm.testing.TestWordSpout"
    parallelism: 1

# bolt definitions
bolts:
  - id: "bolt-1"
    className: "backtype.storm.testing.TestWordCounter"
    parallelism: 1
  - id: "bolt-2"
    className: "org.apache.storm.flux.test.LogInfoBolt"
    parallelism: 1

#stream definitions
streams:
  - name: "spout-1 --> bolt-1" # name isn't used (placeholder for logging, UI, etc.)
    from: "spout-1"
    to: "bolt-1"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "bolt-1 --> bolt2"
    from: "bolt-1"
    to: "bolt-2"
    grouping:
      type: SHUFFLE


```

## Components
Components are essentially named object instances that are made available as configuration options for spouts and
bolts. If you are familiar with the Spring framework, components are roughly analagous to Spring beans.

Every component is identified, at a minimum, by a unique identifier (String) and a class name (String). For example,
the following will make an instance of the `storm.kafka.StringScheme` class available as a reference under the key
`"stringScheme"` . This assumes the `storm.kafka.StringScheme` has a default constructor.

```yaml
components:
  - id: "stringScheme"
    className: "storm.kafka.StringScheme"
```

To specify constructor arguments, you would do somthing like this:

```yaml
components:
  - id: "stringScheme"
    className: "storm.kafka.StringScheme"

  - id: "stringMultiScheme"
    className: "backtype.storm.spout.SchemeAsMultiScheme"
    constructorArgs:
      - ref: "stringScheme"
```

In this example the `stringScheme` object is used as a reference in the arguments to the
`backtype.storm.spout.SchemeAsMultiStream` constructor.

## Spouts

Shell spout example:

```
spouts:
  - id: "sentence-spout"
    className: "org.apache.storm.flux.spouts.GenericShellSpout"
    # shell spout constructor takes 2 arguments: String[], String[]
    constructorArgs:
      # command line
      - ["node", "randomsentence.js"]
      # output fields
      - ["word"]
    parallelism: 1
```

Kafka spout example:

```yaml
components:
  - id: "stringScheme"
    className: "storm.kafka.StringScheme"

  - id: "stringMultiScheme"
    className: "backtype.storm.spout.SchemeAsMultiScheme"
    constructorArgs:
      - ref: "stringScheme"

  - id: "zkHosts"
    className: "storm.kafka.ZkHosts"
    constructorArgs:
      - "localhost:2181"

# Alternative kafka config
#  - id: "kafkaConfig"
#    className: "storm.kafka.KafkaConfig"
#    constructorArgs:
#      # brokerHosts
#      - ref: "zkHosts"
#      # topic
#      - "myKafkaTopic"
#      # clientId (optional)
#      - "myKafkaClientId"

  - id: "spoutConfig"
    className: "storm.kafka.SpoutConfig"
    constructorArgs:
      # brokerHosts
      - ref: "zkHosts"
      # topic
      - "myKafkaTopic"
      # zkRoot
      - "/kafkaSpout"
      # id
      - "myId"
    properties:
      - name: "forceFromStart"
        value: true
      - name: "scheme"
        ref: "stringMultiScheme"

config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "kafka-spout"
    className: "storm.kafka.KafkaSpout"
    constructorArgs:
      - ref: "spoutConfig"

```

## Bolts

```yaml
# bolt definitions
bolts:
  - id: "splitsentence"
    className: "org.apache.storm.flux.bolts.GenericShellBolt"
    constructorArgs:
      # command line
      - ["python", "splitsentence.py"]
      # output fields
      - ["word"]
    parallelism: 1
    # ...

  - id: "log"
    className: "org.apache.storm.flux.test.LogInfoBolt"
    parallelism: 1
    # ...

  - id: "count"
    className: "backtype.storm.testing.TestWordCounter"
    parallelism: 1
    # ...
```
## Streams

```yaml
#stream definitions
# stream definitions define connections between spouts and bolts.
# note that such connections can be cyclical
# custom stream groupings are also supported

streams:
  - name: "kafka --> split" # name isn't used (placeholder for logging, UI, etc.)
    from: "kafka-spout"
    to: "splitsentence"
    grouping:
      type: SHUFFLE

  - name: "split --> count"
    from: "splitsentence"
    to: "count"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "count --> log"
    from: "count"
    to: "log"
    grouping:
      type: SHUFFLE
```

## Basic Word Count Example

This example uses a spout implemented in JavaScript, a bolt implemented in Python, and a bolt implemented in Java

Topology YAML config:

```yaml
---
name: "shell-topology"
config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "sentence-spout"
    className: "org.apache.storm.flux.spouts.GenericShellSpout"
    # shell spout constructor takes 2 arguments: String[], String[]
    constructorArgs:
      # command line
      - ["node", "randomsentence.js"]
      # output fields
      - ["word"]
    parallelism: 1

# bolt definitions
bolts:
  - id: "splitsentence"
    className: "org.apache.storm.flux.bolts.GenericShellBolt"
    constructorArgs:
      # command line
      - ["python", "splitsentence.py"]
      # output fields
      - ["word"]
    parallelism: 1

  - id: "log"
    className: "org.apache.storm.flux.test.LogInfoBolt"
    parallelism: 1

  - id: "count"
    className: "backtype.storm.testing.TestWordCounter"
    parallelism: 1

#stream definitions
# stream definitions define connections between spouts and bolts.
# note that such connections can be cyclical
# custom stream groupings are also supported

streams:
  - name: "spout --> split" # name isn't used (placeholder for logging, UI, etc.)
    from: "sentence-spout"
    to: "splitsentence"
    grouping:
      type: SHUFFLE

  - name: "split --> count"
    from: "splitsentence"
    to: "count"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "count --> log"
    from: "count"
    to: "log"
    grouping:
      type: SHUFFLE
```

## How do you deal with a spout or bolt that has custom constructor?

Define a list of arguments that should be passed to the constuctor:

```yaml
# spout definitions
spouts:
  - id: "sentence-spout"
    className: "org.apache.storm.flux.spouts.GenericShellSpout"
    # shell spout constructor takes 2 arguments: String[], String[]
    constructorArgs:
      # command line
      - ["node", "randomsentence.js"]
      # output fields
      - ["word"]
    parallelism: 1
```

