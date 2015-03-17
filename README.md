# [flux] (http://en.wikipedia.org/wiki/Flux)

## Definition
**flux** |fləks| _noun_

1. The action or process of flowing or flowing out
2. Continuous change
3. In physics, the rate of flow of a fluid, radiant energy, or particles across a given area
4. A substance mixed with a solid to lower its melting point

## About
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
storm jar mytopology.jar org.apache.storm.flux.Flux --local config.yaml
```

or:

```bash
storm jar mytopology.jar org.apache.storm.flux.Flux --remote config.yaml
```

Another pain point often mentioned is the fact that the wiring for a Topology graph is often tied up in Java code,
and that any changes require recompilation and repackaging of the topology jar file. Flux aims to eliminate that
pain by allowing you to package all your Storm components in a single jar, and use an external text file to define
the layout and configuration of your topologies.

## Disclaimer

This is an experimental project that is rapidly changing. Documentation may be out of date.

## Usage

To use Flux, add it as a dependency and package all your Storm components in a fat jar, then create a YAML document
to define your topology (see below).

```
usage: storm jar <my_topology_uber_jar.jar> org.apache.storm.flux.Flux
             [options] <topology-config.yaml>
 -d,--dry-run         Do not run or deploy the topology. Just build,
                      validate, and print information about the topology.
 -l,--local           Run the topology in local mode.
 -n,--no-splash       Supress the printing of the splash screen.
 -q,--no-detail       Supress the printing of topology details.
 -r,--remote          Deploy the topology to a remote cluster.
 -R,--resource        Treat the supplied path as a classpath resource
                      instead of a file.
 -s,--sleep <sleep>   When running locally, the amount of time to sleep
                      (in ms.) before killing the topology and shutting
                      down the local cluster.
```

### Sample output
```
███████╗██╗     ██╗   ██╗██╗  ██╗
██╔════╝██║     ██║   ██║╚██╗██╔╝
█████╗  ██║     ██║   ██║ ╚███╔╝
██╔══╝  ██║     ██║   ██║ ██╔██╗
██║     ███████╗╚██████╔╝██╔╝ ██╗
╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝
+-         Apache Storm        -+
+-  data FLow User eXperience  -+
Version: 0.1.0-SNAPSHOT
Parsing file: /Users/hsimpson/Projects/donut_domination/storm/shell_test.yaml
---------- TOPOLOGY DETAILS ----------
Name: shell-topology
--------------- SPOUTS ---------------
sentence-spout[1](org.apache.storm.flux.spouts.GenericShellSpout)
---------------- BOLTS ---------------
splitsentence[1](org.apache.storm.flux.bolts.GenericShellBolt)
log[1](org.apache.storm.flux.test.LogInfoBolt)
count[1](backtype.storm.testing.TestWordCounter)
--------------- STREAMS ---------------
sentence-spout --SHUFFLE--> splitsentence
splitsentence --FIELDS--> count
count --SHUFFLE--> log
--------------------------------------
Submitting topology: 'shell-topology' to remote cluster...
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

### Contructor Arguments, References and Properties

####Constructor Arguments
Arguments to a class constructor can be configured by adding `contructorArgs` to a components. `constructorArgs` is a
list of objects that will be passed to the class' constructor. The following example creates an object by calling
the constructor that takes a single string as an argument:

```yaml
  - id: "zkHosts"
    className: "storm.kafka.ZkHosts"
    constructorArgs:
      - "localhost:2181"
```

####References
Each component instance is identified by a unique id that allows it to be used/reused by other components. To
reference an existing components, you specify the id of the component with the `ref` tag.

In the following example, a component with the id `"stringScheme"` is created, and later referenced as a an argument
to another component's constructor:

```yaml
components:
  - id: "stringScheme"
    className: "storm.kafka.StringScheme"

  - id: "stringMultiScheme"
    className: "backtype.storm.spout.SchemeAsMultiScheme"
    constructorArgs:
      - ref: "stringScheme" # component with id "stringScheme" must be declared above.
```
**N.B.:** References can only be used after (below) the object they point to has been declared.

####Properties
In addition to calling contructors with different arguments, Flux also allows you to configure components using
JavaBean-like setter methods and fields declared as `public`:

```yaml
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
```

In the example above, the `properties` declaration will cause Flux to look for a public method in the `SpoutConfig` with the
signature `setForceFromStart(boolean b)` and attempt to invoke it. If a setter method is not found, Flux will then
look for a public instance variable with the name `forceFromStart` and attempt to set its value.

References may also be used as property values.




## Topology Config
The `config` section is simply a map of Storm topology configuration parameters that will be passed to the
`backtype.storm.StormSubmitter` as an instance of the `backtype.storm.Config` class:

```yaml
config:
  topology.workers: 4
  topology.max.spout.pending: 1000
  topology.message.timeout.secs: 30
```

## Spouts and Bolts
Spout and Bolts are configured in their own respective section of the YAML configuration. Spout and Bolt defintions
are extensions to the `component` definition that add a `parallelism` parameter that sets the parallelism to be
used when the topology is deployed on a cluster. Because spout and bolt definitions extend `component` they support
constructor arguments, references, and properties as well.

Shell spout example:

```yaml
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

Bolt Examples:

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
## Streams and Stream Groupings
Streams in Flux are represented as a list of connections (data flow) between the Spouts and Bolts in a topology, and an associated
Grouping definition.

A Stream definition has the following properties:

**`name`:** A name for the connection (optional, currently unused)

**`from`:** The `id` of a Spout or Bolt that is the source (publisher)

**`to`:** The `id` of a Spout or Bolt that is the destination (subscriber)

**`grouping`:** The stream grouping definition for the Stream

A Grouping definition has the following properties:

**`type`:** The type of grouping. One of `ALL`,`CUSTOM`,`DIRECT`,`SHUFFLE`,`LOCAL_OR_SHUFFLE`,`FIELDS`,`GLOBAL`, or `NONE`.

**`streamId`:** The Storm stream ID (Optional. If unspecified will use the default stream)

**`args`:** For the `FIELDS` grouping, a list of field names.

**`customClass`"** For the `CUSTOM` grouping, a definition of custom grouping class instance

The `streams` definition example below sets up a topology with the following wiring:

```
    kafka-spout --> splitsentence --> count --> log
```


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

### Custom Stream Groupings
Custom stream groupings are defined by setting the grouping type to `CUSTOM` and defining a `customClass` parameter
that tells Flux how to instantiate the custom class. The `customClass` definition extends `component`, so it supports
constructor arguments, references, and properties as well.

The example below creates a Stream with an instance of the `backtype.storm.testing.NGrouping` custom stream grouping
class.

```yaml
  - name: "bolt-1 --> bolt2"
    from: "bolt-1"
    to: "bolt-2"
    grouping:
      type: CUSTOM
      customClass:
        className: "backtype.storm.testing.NGrouping"
        constructorArgs:
          - 1
```

## Includes and Overrides
Flux allows you to include the contents of other YAML files, and have them treated as though they were defined in the
same file. Includes may be either files, or classpath resources.

Includes are specified as a list of maps:

```yaml
includes:
  - resource: false
    file: "src/test/resources/configs/shell_test.yaml"
    override: false
```

If the `resource` property is set to `true`, the include will be loaded as a classpath resource from the value of the
`file` attribute, otherwise it will be treated as a regular file.

The `override` property controls how includes affect the values defined in the current file. If `override` is set to
`true`, values in the included file will replace values in the current file being parsed. If `override` is set to
`false`, values in the current file being parsed will take precedence, and the parser will refuse to replace them.

**N.B.:** Includes are not yet recursive. Includes from included files will be ignored.


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


## Trident Support
Currenty Flux only supports the Core Storm API, but support for Trident is planned.