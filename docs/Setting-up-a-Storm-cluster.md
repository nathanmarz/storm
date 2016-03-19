---
layout: documentation
---
This page outlines the steps for getting a Storm cluster up and running. If you're on AWS, you should check out the [storm-deploy](https://github.com/nathanmarz/storm-deploy/wiki) project. [storm-deploy](https://github.com/nathanmarz/storm-deploy/wiki) completely automates the provisioning, configuration, and installation of Storm clusters on EC2. It also sets up Ganglia for you so you can monitor CPU, disk, and network usage.

If you run into difficulties with your Storm cluster, first check for a solution is in the [Troubleshooting](Troubleshooting.html) page. Otherwise, email the mailing list.

Here's a summary of the steps for setting up a Storm cluster:

1. Set up a Zookeeper cluster
2. Install dependencies on Nimbus and worker machines
3. Download and extract a Storm release to Nimbus and worker machines
4. Fill in mandatory configurations into storm.yaml
5. Launch daemons under supervision using "storm" script and a supervisor of your choice

### Set up a Zookeeper cluster

Storm uses Zookeeper for coordinating the cluster. Zookeeper **is not** used for message passing, so the load Storm places on Zookeeper is quite low. Single node Zookeeper clusters should be sufficient for most cases, but if you want failover or are deploying large Storm clusters you may want larger Zookeeper clusters. Instructions for deploying Zookeeper are [here](http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html). 

A few notes about Zookeeper deployment:

1. It's critical that you run Zookeeper under supervision, since Zookeeper is fail-fast and will exit the process if it encounters any error case. See [here](http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_supervision) for more details. 
2. It's critical that you set up a cron to compact Zookeeper's data and transaction logs. The Zookeeper daemon does not do this on its own, and if you don't set up a cron, Zookeeper will quickly run out of disk space. See [here](http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_maintenance) for more details.

### Install dependencies on Nimbus and worker machines

Next you need to install Storm's dependencies on Nimbus and the worker machines. These are:

1. Java 6
2. Python 2.6.6

These are the versions of the dependencies that have been tested with Storm. Storm may or may not work with different versions of Java and/or Python.


### Download and extract a Storm release to Nimbus and worker machines

Next, download a Storm release and extract the zip file somewhere on Nimbus and each of the worker machines. The Storm releases can be downloaded [from here](http://github.com/apache/incubator-storm/downloads).

### Fill in mandatory configurations into storm.yaml

The Storm release contains a file at `conf/storm.yaml` that configures the Storm daemons. You can see the default configuration values [here](https://github.com/apache/incubator-storm/blob/master/conf/defaults.yaml). storm.yaml overrides anything in defaults.yaml. There's a few configurations that are mandatory to get a working cluster:

1) **storm.zookeeper.servers**: This is a list of the hosts in the Zookeeper cluster for your Storm cluster. It should look something like:

```yaml
storm.zookeeper.servers:
  - "111.222.333.444"
  - "555.666.777.888"
```

If the port that your Zookeeper cluster uses is different than the default, you should set **storm.zookeeper.port** as well.

2) **storm.local.dir**: The Nimbus and Supervisor daemons require a directory on the local disk to store small amounts of state (like jars, confs, and things like that). You should create that directory on each machine, give it proper permissions, and then fill in the directory location using this config. For example:

```yaml
storm.local.dir: "/mnt/storm"
```

3) **nimbus.host**: The worker nodes need to know which machine is the master in order to download topology jars and confs. For example:

```yaml
nimbus.host: "111.222.333.44"
```

4) **supervisor.slots.ports**: For each worker machine, you configure how many workers run on that machine with this config. Each worker uses a single port for receiving messages, and this setting defines which ports are open for use. If you define five ports here, then Storm will allocate up to five workers to run on this machine. If you define three ports, Storm will only run up to three. By default, this setting is configured to run 4 workers on the ports 6700, 6701, 6702, and 6703. For example:

```yaml
supervisor.slots.ports:
    - 6700
    - 6701
    - 6702
    - 6703
```

### Launch daemons under supervision using "storm" script and a supervisor of your choice

The last step is to launch all the Storm daemons. It is critical that you run each of these daemons under supervision. Storm is a __fail-fast__ system which means the processes will halt whenever an unexpected error is encountered. Storm is designed so that it can safely halt at any point and recover correctly when the process is restarted. This is why Storm keeps no state in-process -- if Nimbus or the Supervisors restart, the running topologies are unaffected. Here's how to run the Storm daemons:

1. **Nimbus**: Run the command "bin/storm nimbus" under supervision on the master machine.
2. **Supervisor**: Run the command "bin/storm supervisor" under supervision on each worker machine. The supervisor daemon is responsible for starting and stopping worker processes on that machine.
3. **UI**: Run the Storm UI (a site you can access from the browser that gives diagnostics on the cluster and topologies) by running the command "bin/storm ui" under supervision. The UI can be accessed by navigating your web browser to http://{nimbus host}:8080. 

As you can see, running the daemons is very straightforward. The daemons will log to the logs/ directory in wherever you extracted the Storm release.
