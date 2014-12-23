---
layout: about
---

Storm integrates with any queueing system and any database system. Storm's [spout](/apidocs/backtype/storm/spout/ISpout.html) abstraction makes it easy to integrate a new queuing system. Example queue integrations include:

1. [Kestrel](https://github.com/nathanmarz/storm-kestrel)
2. [RabbitMQ / AMQP](https://github.com/Xorlev/storm-amqp-spout)
3. [Kafka](https://github.com/apache/storm/tree/master/external/storm-kafka)
4. [JMS](https://github.com/ptgoetz/storm-jms)
5. [Amazon Kinesis](https://github.com/awslabs/kinesis-storm-spout)

Likewise, integrating Storm with database systems is easy. Simply open a connection to your database and read/write like you normally would. Storm will handle the parallelization, partitioning, and retrying on failures when necessary.