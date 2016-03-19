---
layout: documentation
---
This section of the wiki is dedicated to explaining how Storm is implemented. You should have a good grasp of how to use Storm before reading these sections. 

- [Structure of the codebase](Structure-of-the-codebase.html)
- [Lifecycle of a topology](Lifecycle-of-a-topology.html)
- [Message passing implementation](Message-passing-implementation.html)
- [Acking framework implementation](Acking-framework-implementation.html)
- [Metrics](Metrics.html)
- How transactional topologies work
   - subtopology for TransactionalSpout
   - how state is stored in ZK
   - subtleties around what to do when emitting batches out of order
- Unit testing
  - time simulation
  - complete-topology
  - tracker clusters
