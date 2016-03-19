---
title: Message Passing Implementation
layout: documentation
documentation: true
---
(Note: this walkthrough is out of date as of 0.8.0. 0.8.0 revamped the message passing infrastructure to be based on the Disruptor)

This page walks through how emitting and transferring tuples works in Storm.

- Worker is responsible for message transfer
   - `refresh-connections` is called every "task.refresh.poll.secs" or whenever assignment in ZK changes. It manages connections to other workers and maintains a mapping from task -> worker [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L123)
   - Provides a "transfer function" that is used by tasks to send tuples to other tasks. The transfer function takes in a task id and a tuple, and it serializes the tuple and puts it onto a "transfer queue". There is a single transfer queue for each worker. [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L56)
   - The serializer is thread-safe [code](https://github.com/apache/storm/blob/0.7.1/src/jvm/backtype/storm/serialization/KryoTupleSerializer.java#L26)
   - The worker has a single thread which drains the transfer queue and sends the messages to other workers [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L185)
   - Message sending happens through this protocol: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/messaging/protocol.clj)
   - The implementation for distributed mode uses ZeroMQ [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/messaging/zmq.clj)
   - The implementation for local mode uses in memory Java queues (so that it's easy to use Storm locally without needing to get ZeroMQ installed) [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/messaging/local.clj)
- Receiving messages in tasks works differently in local mode and distributed mode
   - In local mode, the tuple is sent directly to an in-memory queue for the receiving task [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/messaging/local.clj#L21)
   - In distributed mode, each worker listens on a single TCP port for incoming messages and then routes those messages in-memory to tasks. The TCP port is called a "virtual port", because it receives [task id, message] and then routes it to the actual task. [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L204)
      - The virtual port implementation is here: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/zilch/virtual_port.clj)
      - Tasks listen on an in-memory ZeroMQ port for messages from the virtual port [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L201)
        - Bolts listen here: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L489)
        - Spouts listen here: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L382)
- Tasks are responsible for message routing. A tuple is emitted either to a direct stream (where the task id is specified) or a regular stream. In direct streams, the message is only sent if that bolt subscribes to that direct stream. In regular streams, the stream grouping functions are used to determine the task ids to send the tuple to.
  - Tasks have a routing map from {stream id} -> {component id} -> {stream grouping function} [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L198)
  - The "tasks-fn" returns the task ids to send the tuples to for either regular stream emit or direct stream emit [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L207)
  - After getting the output task ids, bolts and spouts use the transfer-fn provided by the worker to actually transfer the tuples
      - Bolt transfer code here: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L429)
      - Spout transfer code here: [code](https://github.com/apache/storm/blob/0.7.1/src/clj/backtype/storm/daemon/task.clj#L329)
