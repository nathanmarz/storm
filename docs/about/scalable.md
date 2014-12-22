---
layout: about
---

Storm topologies are inherently parallel and run across a cluster of machines. Different parts of the topology can be scaled individually by tweaking their parallelism. The "rebalance" command of the "storm" command line client can adjust the parallelism of running topologies on the fly. 

Storm's inherent parallelism means it can process very high throughputs of messages with very low latency. Storm was benchmarked at processing **one million 100 byte messages per second per node** on hardware with the following specs:

 * **Processor:** 2x Intel E5645@2.4Ghz 
 * **Memory:** 24 GB
