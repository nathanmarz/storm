---
title: Scheduler
layout: documentation
documentation: true
---

Storm now has 4 kinds of built-in schedulers: [DefaultScheduler]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/scheduler/DefaultScheduler.java), [IsolationScheduler]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/scheduler/IsolationScheduler.java), [MultitenantScheduler]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/scheduler/multitenant/MultitenantScheduler.java), [ResourceAwareScheduler](Resource_Aware_Scheduler_overview.html). 

## Pluggable scheduler
You can implement your own scheduler to replace the default scheduler to assign executors to workers. You configure the class to use the "storm.scheduler" config in your storm.yaml, and your scheduler must implement the [IScheduler]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/scheduler/IScheduler.java) interface.

## Isolation Scheduler
The isolation scheduler makes it easy and safe to share a cluster among many topologies. The isolation scheduler lets you specify which topologies should be "isolated", meaning that they run on a dedicated set of machines within the cluster where no other topologies will be running. These isolated topologies are given priority on the cluster, so resources will be allocated to isolated topologies if there's competition with non-isolated topologies, and resources will be taken away from non-isolated topologies if necessary to get resources for an isolated topology. Once all isolated topologies are allocated, the remaining machines on the cluster are shared among all non-isolated topologies.

You can configure the isolation scheduler in the Nimbus configuration by setting "storm.scheduler" to "org.apache.storm.scheduler.IsolationScheduler". Then, use the "isolation.scheduler.machines" config to specify how many machines each topology should get. This configuration is a map from topology name to the number of isolated machines allocated to this topology. For example:

```
isolation.scheduler.machines: 
    "my-topology": 8
    "tiny-topology": 1
    "some-other-topology": 3
```

Any topologies submitted to the cluster not listed there will not be isolated. Note that there is no way for a user of Storm to affect their isolation settings – this is only allowed by the administrator of the cluster (this is very much intentional).

The isolation scheduler solves the multi-tenancy problem – avoiding resource contention between topologies – by providing full isolation between topologies. The intention is that "productionized" topologies should be listed in the isolation config, and test or in-development topologies should not. The remaining machines on the cluster serve the dual role of failover for isolated topologies and for running the non-isolated topologies.

