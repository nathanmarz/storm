---
layout: about
---

Storm has a simple and easy to use API. When programming on Storm, you manipulate and transform streams of tuples, and a tuple is a named list of values. Tuples can contain objects of any type; if you want to use a type Storm doesn't know about it's [very easy](/documentation/Serialization.html) to register a serializer for that type.

There are just three abstractions in Storm: spouts, bolts, and topologies. A **spout** is a source of streams in a computation. Typically a spout reads from a queueing broker such as Kestrel, RabbitMQ, or Kafka, but a spout can also generate its own stream or read from somewhere like the Twitter streaming API. Spout implementations already exist for most queueing systems.

A **bolt** processes any number of input streams and produces any number of new output streams. Most of the logic of a computation goes into bolts, such as functions, filters, streaming joins, streaming aggregations, talking to databases, and so on.

A **topology** is a network of spouts and bolts, with each edge in the network representing a bolt subscribing to the output stream of some other spout or bolt. A topology is an arbitrarily complex multi-stage stream computation. Topologies run indefinitely when deployed.

Storm has a "local mode" where a Storm cluster is simulated in-process. This is useful for development and testing. The "storm" command line client is used when ready to submit a topology for execution on an actual cluster.

The [storm-starter](https://github.com/apache/storm/tree/master/examples/storm-starter) project contains example topologies for learning the basics of Storm. Learn more about how to use Storm by reading the [tutorial](/documentation/Tutorial.html) and the [documentation](/documentation/Home.html).