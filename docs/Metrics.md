---
title: Storm Metrics
layout: documentation
documentation: true
---
Storm exposes a metrics interface to report summary statistics across the full topology.
It's used internally to track the numbers you see in the Nimbus UI console: counts of executes and acks; average process latency per bolt; worker heap usage; and so forth.

### Metric Types

Metrics have to implement just one method, `getValueAndReset` -- do any remaining work to find the summary value, and reset back to an initial state. For example, the MeanReducer divides the running total by its running count to find the mean, then initializes both values back to zero.

Storm gives you these metric types:

* [AssignableMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/AssignableMetric.java) -- set the metric to the explicit value you supply. Useful if it's an external value or in the case that you are already calculating the summary statistic yourself.
* [CombinedMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/CombinedMetric.java) -- generic interface for metrics that can be updated associatively. 
* [CountMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/CountMetric.java) -- a running total of the supplied values. Call `incr()` to increment by one, `incrBy(n)` to add/subtract the given number.
  - [MultiCountMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/MultiCountMetric.java) -- a hashmap of count metrics.
* [ReducedMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/ReducedMetric.java)
  - [MeanReducer]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/MeanReducer.java) -- track a running average of values given to its `reduce()` method. (It accepts `Double`, `Integer` or `Long` values, and maintains the internal average as a `Double`.) Despite his reputation, the MeanReducer is actually a pretty nice guy in person.
  - [MultiReducedMetric]({{page.git-blob-base}}/storm-core/src/jvm/backtype/storm/metric/api/MultiReducedMetric.java) -- a hashmap of reduced metrics.


### Metric Consumer


### Build your own metric



### Builtin Metrics

The [builtin metrics]({{page.git-blob-base}}/storm-core/src/clj/backtype/storm/daemon/builtin_metrics.clj) instrument Storm itself.

[builtin_metrics.clj]({{page.git-blob-base}}/storm-core/src/clj/backtype/storm/daemon/builtin_metrics.clj) sets up data structures for the built-in metrics, and facade methods that the other framework components can use to update them. The metrics themselves are calculated in the calling code -- see for example [`ack-spout-msg`]({{page.git-blob-base}}/storm-core/src/clj/backtype/storm/daemon/executor.clj#358)  in `clj/b/s/daemon/daemon/executor.clj`

