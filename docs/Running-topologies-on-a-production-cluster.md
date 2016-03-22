---
title: Running Topologies on a Production Cluster
layout: documentation
documentation: true
---
Running topologies on a production cluster is similar to running in [Local mode](Local-mode.html). Here are the steps:

1) Define the topology (Use [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html) if defining using Java)

2) Use [StormSubmitter](javadocs/org/apache/storm/StormSubmitter.html) to submit the topology to the cluster. `StormSubmitter` takes as input the name of the topology, a configuration for the topology, and the topology itself. For example:

```java
Config conf = new Config();
conf.setNumWorkers(20);
conf.setMaxSpoutPending(5000);
StormSubmitter.submitTopology("mytopology", conf, topology);
```

3) Create a jar containing your code and all the dependencies of your code (except for Storm -- the Storm jars will be added to the classpath on the worker nodes).

If you're using Maven, the [Maven Assembly Plugin](http://maven.apache.org/plugins/maven-assembly-plugin/) can do the packaging for you. Just add this to your pom.xml:

```xml
  <plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
      <descriptorRefs>  
        <descriptorRef>jar-with-dependencies</descriptorRef>
      </descriptorRefs>
      <archive>
        <manifest>
          <mainClass>com.path.to.main.Class</mainClass>
        </manifest>
      </archive>
    </configuration>
  </plugin>
```
Then run mvn assembly:assembly to get an appropriately packaged jar. Make sure you [exclude](http://maven.apache.org/plugins/maven-assembly-plugin/examples/single/including-and-excluding-artifacts.html) the Storm jars since the cluster already has Storm on the classpath.

4) Submit the topology to the cluster using the `storm` client, specifying the path to your jar, the classname to run, and any arguments it will use:

`storm jar path/to/allmycode.jar org.me.MyTopology arg1 arg2 arg3`

`storm jar` will submit the jar to the cluster and configure the `StormSubmitter` class to talk to the right cluster. In this example, after uploading the jar `storm jar` calls the main function on `org.me.MyTopology` with the arguments "arg1", "arg2", and "arg3".

You can find out how to configure your `storm` client to talk to a Storm cluster on [Setting up development environment](Setting-up-development-environment.html).

### Common configurations

There are a variety of configurations you can set per topology. A list of all the configurations you can set can be found [here](javadocs/org/apache/storm/Config.html). The ones prefixed with "TOPOLOGY" can be overridden on a topology-specific basis (the other ones are cluster configurations and cannot be overridden). Here are some common ones that are set for a topology:

1. **Config.TOPOLOGY_WORKERS**: This sets the number of worker processes to use to execute the topology. For example, if you set this to 25, there will be 25 Java processes across the cluster executing all the tasks. If you had a combined 150 parallelism across all components in the topology, each worker process will have 6 tasks running within it as threads.
2. **Config.TOPOLOGY_ACKER_EXECUTORS**: This sets the number of executors that will track tuple trees and detect when a spout tuple has been fully processed. Ackers are an integral part of Storm's reliability model and you can read more about them on [Guaranteeing message processing](Guaranteeing-message-processing.html). By not setting this variable or setting it as null, Storm will set the number of acker executors to be equal to the number of workers configured for this topology. If this variable is set to 0, then Storm will immediately ack tuples as soon as they come off the spout, effectively disabling reliability.
3. **Config.TOPOLOGY_MAX_SPOUT_PENDING**: This sets the maximum number of spout tuples that can be pending on a single spout task at once (pending means the tuple has not been acked or failed yet). It is highly recommended you set this config to prevent queue explosion.
4. **Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS**: This is the maximum amount of time a spout tuple has to be fully completed before it is considered failed. This value defaults to 30 seconds, which is sufficient for most topologies. See [Guaranteeing message processing](Guaranteeing-message-processing.html) for more information on how Storm's reliability model works.
5. **Config.TOPOLOGY_SERIALIZATIONS**: You can register more serializers to Storm using this config so that you can use custom types within tuples.


### Killing a topology

To kill a topology, simply run:

`storm kill {stormname}`

Give the same name to `storm kill` as you used when submitting the topology.

Storm won't kill the topology immediately. Instead, it deactivates all the spouts so that they don't emit any more tuples, and then Storm waits Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS seconds before destroying all the workers. This gives the topology enough time to complete any tuples it was processing when it got killed.

### Updating a running topology

To update a running topology, the only option currently is to kill the current topology and resubmit a new one. A planned feature is to implement a `storm swap` command that swaps a running topology with a new one, ensuring minimal downtime and no chance of both topologies processing tuples at the same time. 

### Monitoring topologies

The best place to monitor a topology is using the Storm UI. The Storm UI provides information about errors happening in tasks and fine-grained stats on the throughput and latency performance of each component of each running topology.

You can also look at the worker logs on the cluster machines.
