---
layout: documentation
---
Storm has a variety of configurations for tweaking the behavior of nimbus, supervisors, and running topologies. Some configurations are system configurations and cannot be modified on a topology by topology basis, whereas other configurations can be modified per topology. 

Every configuration has a default value defined in [defaults.yaml](https://github.com/apache/incubator-storm/blob/master/conf/defaults.yaml) in the Storm codebase. You can override these configurations by defining a storm.yaml in the classpath of Nimbus and the supervisors. Finally, you can define a topology-specific configuration that you submit along with your topology when using [StormSubmitter](javadocs/backtype/storm/StormSubmitter.html). However, the topology-specific configuration can only override configs prefixed with "TOPOLOGY".

Storm 0.7.0 and onwards lets you override configuration on a per-bolt/per-spout basis. The only configurations that can be overriden this way are:

1. "topology.debug"
2. "topology.max.spout.pending"
3. "topology.max.task.parallelism"
4. "topology.kryo.register": This works a little bit differently than the other ones, since the serializations will be available to all components in the topology. More details on [Serialization](Serialization.html). 

The Java API lets you specify component specific configurations in two ways:

1. *Internally:* Override `getComponentConfiguration` in any spout or bolt and return the component-specific configuration map.
2. *Externally:* `setSpout` and `setBolt` in `TopologyBuilder` return an object with methods `addConfiguration` and `addConfigurations` that can be used to override the configurations for the component.

The preference order for configuration values is defaults.yaml < storm.yaml < topology specific configuration < internal component specific configuration < external component specific configuration. 


**Resources:**

* [Config](javadocs/backtype/storm/Config.html): a listing of all configurations as well as a helper class for creating topology specific configurations
* [defaults.yaml](https://github.com/apache/incubator-storm/blob/master/conf/defaults.yaml): the default values for all configurations
* [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html): explains how to create and configure a Storm cluster
* [Running topologies on a production cluster](Running-topologies-on-a-production-cluster.html): lists useful configurations when running topologies on a cluster
* [Local mode](Local-mode.html): lists useful configurations when using local mode
