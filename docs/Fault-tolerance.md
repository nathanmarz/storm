---
layout: documentation
---
This page explains the design details of Storm that make it a fault-tolerant system.

## What happens when a worker dies?

When a worker dies, the supervisor will restart it. If it continuously fails on startup and is unable to heartbeat to Nimbus, Nimbus will reassign the worker to another machine.

## What happens when a node dies?

The tasks assigned to that machine will time-out and Nimbus will reassign those tasks to other machines.

## What happens when Nimbus or Supervisor daemons die?

The Nimbus and Supervisor daemons are designed to be fail-fast (process self-destructs whenever any unexpected situation is encountered) and stateless (all state is kept in Zookeeper or on disk). As described in [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html), the Nimbus and Supervisor daemons must be run under supervision using a tool like daemontools or monit. So if the Nimbus or Supervisor daemons die, they restart like nothing happened.

Most notably, no worker processes are affected by the death of Nimbus or the Supervisors. This is in contrast to Hadoop, where if the JobTracker dies, all the running jobs are lost. 

## Is Nimbus a single point of failure?

If you lose the Nimbus node, the workers will still continue to function. Additionally, supervisors will continue to restart workers if they die. However, without Nimbus, workers won't be reassigned to other machines when necessary (like if you lose a worker machine). 

So the answer is that Nimbus is "sort of" a SPOF. In practice, it's not a big deal since nothing catastrophic happens when the Nimbus daemon dies. There are plans to make Nimbus highly available in the future.

## How does Storm guarantee data processing?

Storm provides mechanisms to guarantee data processing even if nodes die or messages are lost. See [Guaranteeing message processing](Guaranteeing-message-processing.html) for the details.
