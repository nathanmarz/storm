# Flux Examples
A collection of examples illustrating various capabilities.

## Building From Source and Running

Checkout the projects source and perform a top level Maven build (i.e. from the `flux` directory):

```bash
git clone https://github.com/ptgoetz/flux.git
cd flux
mvn install
```

This will create a shaded (i.e. "fat" or "uber") jar in the `flux-examples/target` directory that can run/deployed with
the `storm` command:

```bash
cd flux-examples
storm jar ./target/flux-examples-0.2.3-SNAPSHOT.jar org.apache.storm.flux.Flux --local ./src/main/resources/simple_wordcount.yaml
```

The example YAML files are also packaged in the examples jar, so they can also be referenced with Flux's `--resource`
command line switch:

```bash
storm jar ./target/flux-examples-0.2.3-SNAPSHOT.jar org.apache.storm.flux.Flux --local --resource /simple_wordcount.yaml
```

## Available Examples

### [simple_wordcount.yaml](src/main/resources/simple_wordcount.yaml)

This is a very basic wordcount example using Java spouts and bolts. It simply logs the running count of each word
received.

### [multilang.yaml](src/main/resources/multilang.yaml)

Another wordcount example that uses a spout written in JavaScript (node.js), a bolt written in Python, and two bolts
written in java.

### [kafka_spout.yaml](src/main/resources/kafka_spout.yaml)
This example illustrates how to configure Storm's `storm-kafka` spout using Flux YAML DSL `components`, `references`,
and `constructor arguments` constructs.

### [simple_hdfs.yaml](src/main/resources/simple_hdfs.yaml)

This example demonstrates using Flux to setup a storm-hdfs bolt to write to an HDFS cluster. It also demonstrates Flux's
variable substitution/filtering feature.

To run the `simple_hdfs.yaml` example, copy the `hdfs_bolt.properties` file to a convenient location and change, at
least, the property `hdfs.url` to point to a HDFS cluster. Then you can run the example something like:

```bash
storm jar ./target/flux-examples-0.2.3-SNAPSHOT.jar org.apache.storm.flux.Flux --local ./src/main/resources/simple_hdfs.yaml --filter my_hdfs_bolt.properties
```

### [simple_hbase.yaml](src/main/resources/simple_hbase.yaml)

This example illustrates how to use Flux to setup a storm-hbase bolt to write to HBase.

In order to use this example, you will need to edit the `src/main resrouces/hbase-site.xml` file to reflect your HBase
environment, and then rebuild the topology jar.

You can do so by running the following Maven command in the `flux-examples` directory:

```bash
mvn clean install
```