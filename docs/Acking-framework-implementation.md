---
layout: documentation
---
[Storm's acker](https://github.com/apache/incubator-storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L28) tracks completion of each tupletree with a checksum hash: each time a tuple is sent, its value is XORed into the checksum, and each time a tuple is acked its value is XORed in again. If all tuples have been successfully acked, the checksum will be zero (the odds that the checksum will be zero otherwise are vanishingly small).

You can read a bit more about the [reliability mechanism](Guaranteeing-message-processing.html#what-is-storms-reliability-api) elsewhere on the wiki -- this explains the internal details.

### acker `execute()`

The acker is actually a regular bolt, with its  [execute method](https://github.com/apache/incubator-storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L36) defined withing `mk-acker-bolt`.  When a new tupletree is born, the spout sends the XORed edge-ids of each tuple recipient, which the acker records in its `pending` ledger. Every time an executor acks a tuple, the acker receives a partial checksum that is the XOR of the tuple's own edge-id (clearing it from the ledger) and the edge-id of each downstream tuple the executor emitted (thus entering them into the ledger).

This is accomplished as follows.

On a tick tuple, just advance pending tupletree checksums towards death and return. Otherwise, update or create the record for this tupletree:

* on init: initialize with the given checksum value, and record the spout's id for later.
* on ack:  xor the partial checksum into the existing checksum value
* on fail: just mark it as failed

Next, [put the record](https://github.com/apache/incubator-storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L50)),  into the RotatingMap (thus resetting is countdown to expiry) and take action:

* if the total checksum is zero, the tupletree is complete: remove it from the pending collection and notify the spout of success
* if the tupletree has failed, it is also complete:   remove it from the pending collection and notify the spout of failure

Finally, pass on an ack of our own.

### Pending tuples and the `RotatingMap`

The acker stores pending tuples in a [`RotatingMap`](https://github.com/apache/incubator-storm/blob/master/storm-core/src/jvm/backtype/storm/utils/RotatingMap.java#L19), a simple device used in several places within Storm to efficiently time-expire a process.

The RotatingMap behaves as a HashMap, and offers the same O(1) access guarantees.

Internally, it holds several HashMaps ('buckets') of its own, each holding a cohort of records that will expire at the same time.  Let's call the longest-lived bucket death row, and the most recent the nursery. Whenever a value is `.put()` to the RotatingMap, it is relocated to the nursery -- and removed from any other bucket it might have been in (effectively resetting its death clock).

Whenever its owner calls `.rotate()`, the RotatingMap advances each cohort one step further towards expiration. (Typically, Storm objects call rotate on every receipt of a system tick stream tuple.) If there are any key-value pairs in the former death row bucket, the RotatingMap invokes a callback (given in the constructor) for each key-value pair, letting its owner take appropriate action (eg, failing a tuple.

