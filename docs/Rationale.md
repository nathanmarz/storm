---
layout: documentation
---
The past decade has seen a revolution in data processing. MapReduce, Hadoop, and related technologies have made it possible to store and process data at scales previously unthinkable. Unfortunately, these data processing technologies are not realtime systems, nor are they meant to be. There's no hack that will turn Hadoop into a realtime system; realtime data processing has a fundamentally different set of requirements than batch processing.

However, realtime data processing at massive scale is becoming more and more of a requirement for businesses. The lack of a "Hadoop of realtime" has become the biggest hole in the data processing ecosystem.

Storm fills that hole.

Before Storm, you would typically have to manually build a network of queues and workers to do realtime processing. Workers would process messages off a queue, update databases, and send new messages to other queues for further processing. Unfortunately, this approach has serious limitations:

1. **Tedious**: You spend most of your development time configuring where to send messages, deploying workers, and deploying intermediate queues. The realtime processing logic that you care about corresponds to a relatively small percentage of your codebase.
2. **Brittle**: There's little fault-tolerance. You're responsible for keeping each worker and queue up.
3. **Painful to scale**: When the message throughput get too high for a single worker or queue, you need to partition how the data is spread around. You need to reconfigure the other workers to know the new locations to send messages. This introduces moving parts and new pieces that can fail.

Although the queues and workers paradigm breaks down for large numbers of messages, message processing is clearly the fundamental paradigm for realtime computation. The question is: how do you do it in a way that doesn't lose data, scales to huge volumes of messages, and is dead-simple to use and operate?

Storm satisfies these goals. 

## Why Storm is important

Storm exposes a set of primitives for doing realtime computation. Like how MapReduce greatly eases the writing of parallel batch processing, Storm's primitives greatly ease the writing of parallel realtime computation.

The key properties of Storm are:

1. **Extremely broad set of use cases**: Storm can be used for processing messages and updating databases (stream processing), doing a continuous query on data streams and streaming the results into clients (continuous computation), parallelizing an intense query like a search query on the fly (distributed RPC), and more. Storm's small set of primitives satisfy a stunning number of use cases.
2. **Scalable**: Storm scales to massive numbers of messages per second. To scale a topology, all you have to do is add machines and increase the parallelism settings of the topology. As an example of Storm's scale, one of Storm's initial applications processed 1,000,000 messages per second on a 10 node cluster, including hundreds of database calls per second as part of the topology. Storm's usage of Zookeeper for cluster coordination makes it scale to much larger cluster sizes.
3. **Guarantees no data loss**: A realtime system must have strong guarantees about data being successfully processed. A system that drops data has a very limited set of use cases. Storm guarantees that every message will be processed, and this is in direct contrast with other systems like S4. 
4. **Extremely robust**: Unlike systems like Hadoop, which are notorious for being difficult to manage, Storm clusters just work. It is an explicit goal of the Storm project to make the user experience of managing Storm clusters as painless as possible.
5. **Fault-tolerant**: If there are faults during execution of your computation, Storm will reassign tasks as necessary. Storm makes sure that a computation can run forever (or until you kill the computation).
6. **Programming language agnostic**: Robust and scalable realtime processing shouldn't be limited to a single platform. Storm topologies and processing components can be defined in any language, making Storm accessible to nearly anyone.
