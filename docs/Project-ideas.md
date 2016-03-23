---
layout: documentation
---
 * **DSLs for non-JVM languages:** These DSL's should be all-inclusive and not require any Java for the creation of topologies, spouts, or bolts. Since topologies are [Thrift](http://thrift.apache.org/) structs, Nimbus is a Thrift service, and bolts can be written in any language, this is possible.
 * **Online machine learning algorithms:** Something like [Mahout](http://mahout.apache.org/) but for online algorithms
 * **Suite of performance benchmarks:** These benchmarks should test Storm's performance on CPU and IO intensive workloads. There should be benchmarks for different classes of applications, such as stream processing (where throughput is the priority) and distributed RPC (where latency is the priority). 
