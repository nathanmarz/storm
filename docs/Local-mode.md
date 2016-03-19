---
layout: documentation
---
Local mode simulates a Storm cluster in process and is useful for developing and testing topologies. Running topologies in local mode is similar to running topologies [on a cluster](Running-topologies-on-a-production-cluster.html). 

To create an in-process cluster, simply use the `LocalCluster` class. For example:

```java
import backtype.storm.LocalCluster;

LocalCluster cluster = new LocalCluster();
```

You can then submit topologies using the `submitTopology` method on the `LocalCluster` object. Just like the corresponding method on [StormSubmitter](javadocs/backtype/storm/StormSubmitter.html), `submitTopology` takes a name, a topology configuration, and the topology object. You can then kill a topology using the `killTopology` method which takes the topology name as an argument.

To shutdown a local cluster, simple call:

```java
cluster.shutdown();
```

### Common configurations for local mode

You can see a full list of configurations [here](javadocs/backtype/storm/Config.html).

1. **Config.TOPOLOGY_MAX_TASK_PARALLELISM**: This config puts a ceiling on the number of threads spawned for a single component. Oftentimes production topologies have a lot of parallelism (hundreds of threads) which places unreasonable load when trying to test the topology in local mode. This config lets you easy control that parallelism.
2. **Config.TOPOLOGY_DEBUG**: When this is set to true, Storm will log a message every time a tuple is emitted from any spout or bolt. This is extremely useful for debugging.
