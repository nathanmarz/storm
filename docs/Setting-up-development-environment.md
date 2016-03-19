---
layout: documentation
---
This page outlines what you need to do to get a Storm development environment set up. In summary, the steps are:

1. Download a [Storm release](/releases.html) , unpack it, and put the unpacked `bin/` directory on your PATH
2. To be able to start and stop topologies on a remote cluster, put the cluster information in `~/.storm/storm.yaml`

More detail on each of these steps is below.

### What is a development environment?

Storm has two modes of operation: local mode and remote mode. In local mode, you can develop and test topologies completely in process on your local machine. In remote mode, you submit topologies for execution on a cluster of machines.

A Storm development environment has everything installed so that you can develop and test Storm topologies in local mode, package topologies for execution on a remote cluster, and submit/kill topologies on a remote cluster.

Let's quickly go over the relationship between your machine and a remote cluster. A Storm cluster is managed by a master node called "Nimbus". Your machine communicates with Nimbus to submit code (packaged as a jar) and topologies for execution on the cluster, and Nimbus will take care of distributing that code around the cluster and assigning workers to run your topology. Your machine uses a command line client called `storm` to communicate with Nimbus. The `storm` client is only used for remote mode; it is not used for developing and testing topologies in local mode.

### Installing a Storm release locally

If you want to be able to submit topologies to a remote cluster from your machine, you should install a Storm release locally. Installing a Storm release will give you the `storm` client that you can use to interact with remote clusters. To install Storm locally, download a release [from here](/releases.html) and unzip it somewhere on your computer. Then add the unpacked `bin/` directory onto your `PATH` and make sure the `bin/storm` script is executable.

Installing a Storm release locally is only for interacting with remote clusters. For developing and testing topologies in local mode, it is recommended that you use Maven to include Storm as a dev dependency for your project. You can read more about using Maven for this purpose on [Maven](Maven.html). 

### Starting and stopping topologies on a remote cluster

The previous step installed the `storm` client on your machine which is used to communicate with remote Storm clusters. Now all you have to do is tell the client which Storm cluster to talk to. To do this, all you have to do is put the host address of the master in the `~/.storm/storm.yaml` file. It should look something like this:

```
nimbus.host: "123.45.678.890"
```

Alternatively, if you use the [storm-deploy](https://github.com/nathanmarz/storm-deploy) project to provision Storm clusters on AWS, it will automatically set up your ~/.storm/storm.yaml file. You can manually attach to a Storm cluster (or switch between multiple clusters) using the "attach" command, like so:

```
lein run :deploy --attach --name mystormcluster
```

More information is on the storm-deploy [wiki](https://github.com/nathanmarz/storm-deploy/wiki)
