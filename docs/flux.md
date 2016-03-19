---
title: Flux
layout: documentation
documentation: true
---

A framework for creating and deploying Apache Storm streaming computations with less friction.

## Definition
**flux** |fləks| _noun_

1. The action or process of flowing or flowing out
2. Continuous change
3. In physics, the rate of flow of a fluid, radiant energy, or particles across a given area
4. A substance mixed with a solid to lower its melting point

## Rationale
Bad things happen when configuration is hard-coded. No one should have to recompile or repackage an application in
order to change configuration.

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
and that any changes require recompilation and repackaging of the topology jar file. Flux aims to alleviate that
pain by allowing you to package all your Storm components in a single jar, and use an external text file to define
the layout and configuration of your topologies.

## Features

 * Easily configure and deploy Storm topologies (Both Storm core and Microbatch API) without embedding configuration
   in your topology code
 * Support for existing topology code (see below)
 * Define Storm Core API (Spouts/Bolts) using a flexible YAML DSL
 * YAML DSL support for most Storm components (storm-kafka, storm-hdfs, storm-hbase, etc.)
 * Convenient support for multi-lang components
 * External property substitution/filtering for easily switching between configurations/environments (similar to Maven-style
   `${variable.name}` substitution)

## Usage

To use Flux, add it as a dependency and package all your Storm components in a fat jar, then create a YAML document
to define your topology (see below for YAML configuration options).

### Building from Source
The easiest way to use Flux, is to add it as a Maven dependency in you project as described below.

If you would like to build Flux from source and run the unit/integration tests, you will need the following installed
on your system:

* Python 2.6.x or later
* Node.js 0.10.x or later

#### Building with unit tests enabled:

```
mvn clean install
```

#### Building with unit tests disabled:
If you would like to build Flux without installing Python or Node.js you can simply skip the unit tests:

```
mvn clean install -DskipTests=true
```

Note that if you plan on using Flux to deploy topologies to a remote cluster, you will still need to have Python
installed since it is required by Apache Storm.


#### Building with integration tests enabled:

```
mvn clean install -DskipIntegration=false
```


### Packaging with Maven
To enable Flux for your Storm components, you need to add it as a dependency such that it's included in the Storm
topology jar. This can be accomplished with the Maven shade plugin (preferred) or the Maven assembly plugin (not
recommended).

#### Flux Maven Dependency
The current version of Flux is available in Maven Central at the following coordinates:
```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>flux-core</artifactId>
    <version>${storm.version}</version>
</dependency>
```

#### Creating a Flux-Enabled Topology JAR
The example below illustrates Flux usage with the Maven shade plugin:

 ```xml
<!-- include Flux and user dependencies in the shaded jar -->
<dependencies>
    <!-- Flux include -->
    <dependency>
        <groupId>org.apache.storm</groupId>
        <artifactId>flux-core</artifactId>
        <version>${storm.version}</version>
    </dependency>

    <!-- add user dependencies here... -->

</dependencies>
<!-- create a fat jar that includes all dependencies -->
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>1.4</version>
            <configuration>
                <createDependencyReducedPom>true</createDependencyReducedPom>
            </configuration>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <transformers>
                            <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>org.apache.storm.flux.Flux</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
 ```

### Deploying and Running a Flux Topology
Once your topology components are packaged with the Flux dependency, you can run different topologies either locally
or remotely using the `storm jar` command. For example, if your fat jar is named `myTopology-0.1.0-SNAPSHOT.jar` you
could run it locally with the command:


```bash
storm jar myTopology-0.1.0-SNAPSHOT.jar org.apache.storm.flux.Flux --local my_config.yaml

```

### Command line options
```
usage: storm jar <my_topology_uber_jar.jar> org.apache.storm.flux.Flux
             [options] <topology-config.yaml>
 -d,--dry-run                 Do not run or deploy the topology. Just
                              build, validate, and print information about
                              the topology.
 -e,--env-filter              Perform environment variable substitution.
                              Replace keys identified with `${ENV-[NAME]}`
                              will be replaced with the corresponding
                              `NAME` environment value
 -f,--filter <file>           Perform property substitution. Use the
                              specified file as a source of properties,
                              and replace keys identified with {$[property
                              name]} with the value defined in the
                              properties file.
 -i,--inactive                Deploy the topology, but do not activate it.
 -l,--local                   Run the topology in local mode.
 -n,--no-splash               Suppress the printing of the splash screen.
 -q,--no-detail               Suppress the printing of topology details.
 -r,--remote                  Deploy the topology to a remote cluster.
 -R,--resource                Treat the supplied path as a classpath
                              resource instead of a file.
 -s,--sleep <ms>              When running locally, the amount of time to
                              sleep (in ms.) before killing the topology
                              and shutting down the local cluster.
 -z,--zookeeper <host:port>   When running in local mode, use the
                              ZooKeeper at the specified <host>:<port>
                              instead of the in-process ZooKeeper.
                              (requires Storm 0.9.3 or later)
```

**NOTE:** Flux tries to avoid command line switch collision with the `storm` command, and allows any other command line
switches to pass through to the `storm` command.

For example, you can use the `storm` command switch `-c` to override a topology configuration property. The following
example command will run Flux and override the `nimbus.seeds` configuration:

```bash
storm jar myTopology-0.1.0-SNAPSHOT.jar org.apache.storm.flux.Flux --remote my_config.yaml -c 'nimbus.seeds=["localhost"]'
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
Version: 0.3.0
Parsing file: /Users/hsimpson/Projects/donut_domination/storm/shell_test.yaml
---------- TOPOLOGY DETAILS ----------
Name: shell-topology
--------------- SPOUTS ---------------
sentence-spout[1](org.apache.storm.flux.spouts.GenericShellSpout)
---------------- BOLTS ---------------
splitsentence[1](org.apache.storm.flux.bolts.GenericShellBolt)
log[1](org.apache.storm.flux.wrappers.bolts.LogInfoBolt)
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
  3. **EITHER** (A DSL topology definition):
      * A list of spouts, each identified by a unique ID
      * A list of bolts, each identified by a unique ID
      * A list of "stream" objects representing a flow of tuples between spouts and bolts
  4. **OR** (A JVM class that can produce a `backtype.storm.generated.StormTopology` instance:
      * A `topologySource` definition.



For example, here is a simple definition of a wordcount topology using the YAML DSL:

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
    className: "org.apache.storm.flux.wrappers.bolts.LogInfoBolt"
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
## Property Substitution/Filtering
It's common for developers to want to easily switch between configurations, for example switching deployment between
a development environment and a production environment. This can be accomplished by using separate YAML configuration
files, but that approach would lead to unnecessary duplication, especially in situations where the Storm topology
does not change, but configuration settings such as host names, ports, and parallelism paramters do.

For this case, Flux offers properties filtering to allow you two externalize values to a `.properties` file and have
them substituted before the `.yaml` file is parsed.

To enable property filtering, use the `--filter` command line option and specify a `.properties` file. For example,
if you invoked flux like so:

```bash
storm jar myTopology-0.1.0-SNAPSHOT.jar org.apache.storm.flux.Flux --local my_config.yaml --filter dev.properties
```
With the following `dev.properties` file:

```properties
kafka.zookeeper.hosts: localhost:2181
```

You would then be able to reference those properties by key in your `.yaml` file using `${}` syntax:

```yaml
  - id: "zkHosts"
    className: "storm.kafka.ZkHosts"
    constructorArgs:
      - "${kafka.zookeeper.hosts}"
```

In this case, Flux would replace `${kafka.zookeeper.hosts}` with `localhost:2181` before parsing the YAML contents.

### Environment Variable Substitution/Filtering
Flux also allows environment variable substitution. For example, if an environment variable named `ZK_HOSTS` if defined,
you can reference it in a Flux YAML file with the following syntax:

```
${ENV-ZK_HOSTS}
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

### Contructor Arguments, References, Properties and Configuration Methods

####Constructor Arguments
Arguments to a class constructor can be configured by adding a `contructorArgs` element to a components.
`constructorArgs` is a list of objects that will be passed to the class' constructor. The following example creates an
object by calling the constructor that takes a single string as an argument:

```yaml
  - id: "zkHosts"
    className: "storm.kafka.ZkHosts"
    constructorArgs:
      - "localhost:2181"
```

####References
Each component instance is identified by a unique id that allows it to be used/reused by other components. To
reference an existing component, you specify the id of the component with the `ref` tag.

In the following example, a component with the id `"stringScheme"` is created, and later referenced, as a an argument
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
In addition to calling constructors with different arguments, Flux also allows you to configure components using
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

In the example above, the `properties` declaration will cause Flux to look for a public method in the `SpoutConfig` with
the signature `setForceFromStart(boolean b)` and attempt to invoke it. If a setter method is not found, Flux will then
look for a public instance variable with the name `forceFromStart` and attempt to set its value.

References may also be used as property values.

####Configuration Methods
Conceptually, configuration methods are similar to Properties and Constructor Args -- they allow you to invoke an
arbitrary method on an object after it is constructed. Configuration methods are useful for working with classes that
don't expose JavaBean methods or have constructors that can fully configure the object. Common examples include classes
that use the builder pattern for configuration/composition.

The following YAML example creates a bolt and configures it by calling several methods:

```yaml
bolts:
  - id: "bolt-1"
    className: "org.apache.storm.flux.test.TestBolt"
    parallelism: 1
    configMethods:
      - name: "withFoo"
        args:
          - "foo"
      - name: "withBar"
        args:
          - "bar"
      - name: "withFooBar"
        args:
          - "foo"
          - "bar"
```

The signatures of the corresponding methods are as follows:

```java
    public void withFoo(String foo);
    public void withBar(String bar);
    public void withFooBar(String foo, String bar);
```

Arguments passed to configuration methods work much the same way as constructor arguments, and support references as
well.

### Using Java `enum`s in Contructor Arguments, References, Properties and Configuration Methods
You can easily use Java `enum` values as arguments in a Flux YAML file, simply by referencing the name of the `enum`.

For example, [Storm's HDFS module]() includes the following `enum` definition (simplified for brevity):

```java
public static enum Units {
    KB, MB, GB, TB
}
```

And the `org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy` class has the following constructor:

```java
public FileSizeRotationPolicy(float count, Units units)

```
The following Flux `component` definition could be used to call the constructor:

```yaml
  - id: "rotationPolicy"
    className: "org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy"
    constructorArgs:
      - 5.0
      - MB
```

The above definition is functionally equivalent to the following Java code:

```java
// rotate files when they reach 5MB
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
```

## Topology Config
The `config` section is simply a map of Storm topology configuration parameters that will be passed to the
`backtype.storm.StormSubmitter` as an instance of the `backtype.storm.Config` class:

```yaml
config:
  topology.workers: 4
  topology.max.spout.pending: 1000
  topology.message.timeout.secs: 30
```

# Existing Topologies
If you have existing Storm topologies, you can still use Flux to deploy/run/test them. This feature allows you to
leverage Flux Constructor Arguments, References, Properties, and Topology Config declarations for existing topology
classes.

The easiest way to use an existing topology class is to define
a `getTopology()` instance method with one of the following signatures:

```java
public StormTopology getTopology(Map<String, Object> config)
```
or:

```java
public StormTopology getTopology(Config config)
```

You could then use the following YAML to configure your topology:

```yaml
name: "existing-topology"
topologySource:
  className: "org.apache.storm.flux.test.SimpleTopology"
```

If the class you would like to use as a topology source has a different method name (i.e. not `getTopology`), you can
override it:

```yaml
name: "existing-topology"
topologySource:
  className: "org.apache.storm.flux.test.SimpleTopology"
  methodName: "getTopologyWithDifferentMethodName"
```

__N.B.:__ The specified method must accept a single argument of type `java.util.Map<String, Object>` or
`backtype.storm.Config`, and return a `backtype.storm.generated.StormTopology` object.

# YAML DSL
## Spouts and Bolts
Spout and Bolts are configured in their own respective section of the YAML configuration. Spout and Bolt definitions
are extensions to the `component` definition that add a `parallelism` parameter that sets the parallelism  for a
component when the topology is deployed.

Because spout and bolt definitions extend `component` they support constructor arguments, references, and properties as
well.

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
    className: "org.apache.storm.flux.wrappers.bolts.LogInfoBolt"
    parallelism: 1
    # ...

  - id: "count"
    className: "backtype.storm.testing.TestWordCounter"
    parallelism: 1
    # ...
```
## Streams and Stream Groupings
Streams in Flux are represented as a list of connections (Graph edges, data flow, etc.) between the Spouts and Bolts in
a topology, with an associated Grouping definition.

A Stream definition has the following properties:

**`name`:** A name for the connection (optional, currently unused)

**`from`:** The `id` of a Spout or Bolt that is the source (publisher)

**`to`:** The `id` of a Spout or Bolt that is the destination (subscriber)

**`grouping`:** The stream grouping definition for the Stream

A Grouping definition has the following properties:

**`type`:** The type of grouping. One of `ALL`,`CUSTOM`,`DIRECT`,`SHUFFLE`,`LOCAL_OR_SHUFFLE`,`FIELDS`,`GLOBAL`, or `NONE`.

**`streamId`:** The Storm stream ID (Optional. If unspecified will use the default stream)

**`args`:** For the `FIELDS` grouping, a list of field names.

**`customClass`** For the `CUSTOM` grouping, a definition of custom grouping class instance

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
    className: "org.apache.storm.flux.wrappers.bolts.LogInfoBolt"
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


## Micro-Batching (Trident) API Support
Currenty, the Flux YAML DSL only supports the Core Storm API, but support for Storm's micro-batching API is planned.

To use Flux with a Trident topology, define a topology getter method and reference it in your YAML config:

```yaml
name: "my-trident-topology"

config:
  topology.workers: 1

topologySource:
  className: "org.apache.storm.flux.test.TridentTopologySource"
  # Flux will look for "getTopology", this will override that.
  methodName: "getTopologyWithDifferentMethodName"
```
