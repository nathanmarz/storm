---
layout: about
---

Storm guarantees every tuple will be fully processed. One of Storm's core mechanisms is the ability to track the lineage of a tuple as it makes its way through the topology in an extremely efficient way. Read more about how this works [here](/documentation/Guaranteeing-message-processing.html).

Storm's basic abstractions provide an at-least-once processing guarantee, the same guarantee you get when using a queueing system. Messages are only replayed when there are failures.

Using [Trident](/documentation/Trident-tutorial.html), a higher level abstraction over Storm's basic abstractions, you can achieve exactly-once processing semantics.

