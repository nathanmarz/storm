---
title: Command Line Client
layout: documentation
documentation: true
---
This page describes all the commands that are possible with the "storm" command line client. To learn how to set up your "storm" client to talk to a remote cluster, follow the instructions in [Setting up development environment](Setting-up-development-environment.html).

These commands are:

1. jar
1. kill
1. activate
1. deactivate
1. rebalance
1. repl
1. classpath
1. localconfvalue
1. remoteconfvalue
1. nimbus
1. supervisor
1. ui
1. drpc

### jar

Syntax: `storm jar topology-jar-path class ...`

Runs the main method of `class` with the specified arguments. The storm jars and configs in `~/.storm` are put on the classpath. The process is configured so that [StormSubmitter](javadocs/backtype/storm/StormSubmitter.html) will upload the jar at `topology-jar-path` when the topology is submitted.

### kill

Syntax: `storm kill topology-name [-w wait-time-secs]`

Kills the topology with the name `topology-name`. Storm will first deactivate the topology's spouts for the duration of the topology's message timeout to allow all messages currently being processed to finish processing. Storm will then shutdown the workers and clean up their state. You can override the length of time Storm waits between deactivation and shutdown with the -w flag.

### activate

Syntax: `storm activate topology-name`

Activates the specified topology's spouts.

### deactivate

Syntax: `storm deactivate topology-name`

Deactivates the specified topology's spouts.

### rebalance

Syntax: `storm rebalance topology-name [-w wait-time-secs] [-n new-num-workers] [-e component=parallelism]*`

Sometimes you may wish to spread out where the workers for a topology are running. For example, let's say you have a 10 node cluster running 4 workers per node, and then let's say you add another 10 nodes to the cluster. You may wish to have Storm spread out the workers for the running topology so that each node runs 2 workers. One way to do this is to kill the topology and resubmit it, but Storm provides a "rebalance" command that provides an easier way to do this. 

Rebalance will first deactivate the topology for the duration of the message timeout (overridable with the -w flag) and then redistribute the workers evenly around the cluster. The topology will then return to its previous state of activation (so a deactivated topology will still be deactivated and an activated topology will go back to being activated).

The rebalance command can also be used to change the parallelism of a running topology. Use the -n and -e switches to change the number of workers or number of executors of a component respectively.

### repl

Syntax: `storm repl`

Opens up a Clojure REPL with the storm jars and configuration on the classpath. Useful for debugging.

### classpath

Syntax: `storm classpath`

Prints the classpath used by the storm client when running commands.

### localconfvalue

Syntax: `storm localconfvalue conf-name`

Prints out the value for `conf-name` in the local Storm configs. The local Storm configs are the ones in `~/.storm/storm.yaml` merged in with the configs in `defaults.yaml`.

### remoteconfvalue

Syntax: `storm remoteconfvalue conf-name`

Prints out the value for `conf-name` in the cluster's Storm configs. The cluster's Storm configs are the ones in `$STORM-PATH/conf/storm.yaml` merged in with the configs in `defaults.yaml`. This command must be run on a cluster machine.

### nimbus

Syntax: `storm nimbus`

Launches the nimbus daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### supervisor

Syntax: `storm supervisor`

Launches the supervisor daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### ui

Syntax: `storm ui`

Launches the UI daemon. The UI provides a web interface for a Storm cluster and shows detailed stats about running topologies. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### drpc

Syntax: `storm drpc`

Launches a DRPC daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Distributed RPC](Distributed-RPC.html) for more information.
