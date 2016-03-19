---
title: Azue Event Hubs Integration
layout: documentation
documentation: true
---

Storm spout and bolt implementation for Microsoft Azure Eventhubs

### build ###
	mvn clean package

### run sample topology ###
To run the sample topology, you need to modify the config.properties file with
the eventhubs configurations. Here is an example:

	eventhubspout.username = [username: policy name in EventHubs Portal]
	eventhubspout.password = [password: shared access key in EventHubs Portal]
	eventhubspout.namespace = [namespace]
	eventhubspout.entitypath = [entitypath]
	eventhubspout.partitions.count = [partitioncount]

	# if not provided, will use storm's zookeeper settings
	# zookeeper.connectionstring=zookeeper0:2181,zookeeper1:2181,zookeeper2:2181

	eventhubspout.checkpoint.interval = 10
	eventhub.receiver.credits = 1024

Then you can use storm.cmd to submit the sample topology:
	storm jar {jarfile} com.microsoft.eventhubs.samples.EventCount {topologyname} {spoutconffile}
	where the {jarfile} should be: eventhubs-storm-spout-{version}-jar-with-dependencies.jar

### Run EventHubSendClient ###
We have included a simple EventHubs send client for testing purpose. You can run the client like this:
	java -cp .\target\eventhubs-storm-spout-{version}-jar-with-dependencies.jar com.microsoft.eventhubs.client.EventHubSendClient
 	[username] [password] [entityPath] [partitionId] [messageSize] [messageCount]
If you want to send messages to all partitions, use "-1" as partitionId.

### Windows Azure Eventhubs ###
	http://azure.microsoft.com/en-us/services/event-hubs/

