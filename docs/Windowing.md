---
title: Windowing Support in Core Storm
layout: documentation
documentation: true
---

Storm core has support for processing a group of tuples that falls within a window. Windows are specified with the 
following two parameters,

1. Window length - the length or duration of the window
2. Sliding interval - the interval at which the windowing slides

## Sliding Window

Tuples are grouped in windows and window slides every sliding interval. A tuple can belong to more than one window.

For example a time duration based sliding window with length 10 secs and sliding interval of 5 seconds.

```
| e1 e2 | e3 e4 e5 e6 | e7 e8 e9 |...
0       5             10         15    -> time

|<------- w1 -------->|
        |------------ w2 ------->|
```

The window is evaluated every 5 seconds and some of the tuples in the first window overlaps with the second one.
		

## Tumbling Window

Tuples are grouped in a single window based on time or count. Any tuple belongs to only one of the windows.

For example a time duration based tumbling window with length 5 secs.

```
| e1 e2 | e3 e4 e5 e6 | e7 e8 e9 |...
0       5             10         15    -> time
   w1         w2            w3
```

The window is evaluated every five seconds and none of the windows overlap.

Storm supports specifying the window length and sliding intervals as a count of the number of tuples or as a time duration.

The bolt interface `IWindowedBolt` is implemented by bolts that needs windowing support.

```java
public interface IWindowedBolt extends IComponent {
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    /**
     * Process tuples falling within the window and optionally emit 
     * new tuples based on the tuples in the input window.
     */
    void execute(TupleWindow inputWindow);
    void cleanup();
}
```

Every time the window activates, the `execute` method is invoked. The TupleWindow parameter gives access to the current tuples
in the window, the tuples that expired and the new tuples that are added since last window was computed which will be useful 
for efficient windowing computations.

Bolts that needs windowing support typically would extend `BaseWindowedBolt` which has the apis for specifying the
window length and sliding intervals.

E.g. 

```java
public class SlidingWindowBolt extends BaseWindowedBolt {
	private OutputCollector collector;
	
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	this.collector = collector;
    }
	
    @Override
    public void execute(TupleWindow inputWindow) {
	  for(Tuple tuple: inputWindow.get()) {
	    // do the windowing computation
		...
	  }
	  // emit the results
	  collector.emit(new Values(computedValue));
    }
}

public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
     builder.setSpout("spout", new RandomSentenceSpout(), 1);
     builder.setBolt("slidingwindowbolt", 
                     new SlidingWindowBolt().withWindow(new Count(30), new Count(10)),
                     1).shuffleGrouping("spout");
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(1);

    StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
	
}
```

The following window configurations are supported.

```java
withWindow(Count windowLength, Count slidingInterval)
Tuple count based sliding window that slides after `slidingInterval` number of tuples.

withWindow(Count windowLength)
Tuple count based window that slides with every incoming tuple.

withWindow(Count windowLength, Duration slidingInterval)
Tuple count based sliding window that slides after `slidingInterval` time duration.

withWindow(Duration windowLength, Duration slidingInterval)
Time duration based sliding window that slides after `slidingInterval` time duration.

withWindow(Duration windowLength)
Time duration based window that slides with every incoming tuple.

withWindow(Duration windowLength, Count slidingInterval)
Time duration based sliding window configuration that slides after `slidingInterval` number of tuples.

withTumblingWindow(BaseWindowedBolt.Count count)
Count based tumbling window that tumbles after the specified count of tuples.

withTumblingWindow(BaseWindowedBolt.Duration duration)
Time duration based tumbling window that tumbles after the specified time duration.

```

## Tuple timestamp and out of order tuples
By default the timestamp tracked in the window is the time when the tuple is processed by the bolt. The window calculations
are performed based on the processing timestamp. Storm has support for tracking windows based on the source generated timestamp.

```java
/**
* Specify a field in the tuple that represents the timestamp as a long value. If this
* field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
*
* @param fieldName the name of the field that contains the timestamp
*/
public BaseWindowedBolt withTimestampField(String fieldName)
```

The value for the above `fieldName` will be looked up from the incoming tuple and considered for windowing calculations. 
If the field is not present in the tuple an exception will be thrown. Along with the timestamp field name, a time lag parameter 
can also be specified which indicates the max time limit for tuples with out of order timestamps. 

E.g. If the lag is 5 secs and a tuple `t1` arrived with timestamp `06:00:05` no tuples may arrive with tuple timestamp earlier than `06:00:00`. If a tuple
arrives with timestamp 05:59:59 after `t1` and the window has moved past `t1`, it will be treated as a late tuple and not processed. Currently the late
 tuples are just logged in the worker log files at INFO level.

```java
/**
* Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
* cannot be out of order by more than this amount.
*
* @param duration the max lag duration
*/
public BaseWindowedBolt withLag(Duration duration)
```

### Watermarks
For processing tuples with timestamp field, storm internally computes watermarks based on the incoming tuple timestamp. Watermark is 
the minimum of the latest tuple timestamps (minus the lag) across all the input streams. At a higher level this is similar to the watermark concept
used by Flink and Google's MillWheel for tracking event based timestamps.

Periodically (default every sec), the watermark timestamps are emitted and this is considered as the clock tick for the window calculation if 
tuple based timestamps are in use. The interval at which watermarks are emitted can be changed with the below api.
 
```java
/**
* Specify the watermark event generation interval. For tuple based timestamps, watermark events
* are used to track the progress of time
*
* @param interval the interval at which watermark events are generated
*/
public BaseWindowedBolt withWatermarkInterval(Duration interval)
```


When a watermark is received, all windows up to that timestamp will be evaluated.

For example, consider tuple timestamp based processing with following window parameters,

`Window length = 20s, sliding interval = 10s, watermark emit frequency = 1s, max lag = 5s`

```
|-----|-----|-----|-----|-----|-----|-----|
0     10    20    30    40    50    60    70
````

Current ts = `09:00:00`

Tuples `e1(6:00:03), e2(6:00:05), e3(6:00:07), e4(6:00:18), e5(6:00:26), e6(6:00:36)` are received between `9:00:00` and `9:00:01`

At time t = `09:00:01`, watermark w1 = `6:00:31` is emitted since no tuples earlier than `6:00:31` can arrive.

Three windows will be evaluated. The first window end ts (06:00:10) is computed by taking the earliest event timestamp (06:00:03) 
and computing the ceiling based on the sliding interval (10s).

1. `5:59:50 - 06:00:10` with tuples e1, e2, e3
2. `6:00:00 - 06:00:20` with tuples e1, e2, e3, e4
3. `6:00:10 - 06:00:30` with tuples e4, e5

e6 is not evaluated since watermark timestamp `6:00:31` is older than the tuple ts `6:00:36`.

Tuples `e7(8:00:25), e8(8:00:26), e9(8:00:27), e10(8:00:39)` are received between `9:00:01` and `9:00:02`

At time t = `09:00:02` another watermark w2 = `08:00:34` is emitted since no tuples earlier than `8:00:34` can arrive now.

Three windows will be evaluated,

1. `6:00:20 - 06:00:40` with tuples e5, e6 (from earlier batch)
2. `6:00:30 - 06:00:50` with tuple e6 (from earlier batch)
3. `8:00:10 - 08:00:30` with tuples e7, e8, e9

e10 is not evaluated since the tuple ts `8:00:39` is beyond the watermark time `8:00:34`.

The window calculation considers the time gaps and computes the windows based on the tuple timestamp.

## Guarentees
The windowing functionality in storm core currently provides at-least once guarentee. The values emitted from the bolts
`execute(TupleWindow inputWindow)` method are automatically anchored to all the tuples in the inputWindow. The downstream
bolts are expected to ack the received tuple (i.e the tuple emitted from the windowed bolt) to complete the tuple tree. 
If not the tuples will be replayed and the windowing computation will be re-evaluated. 

The tuples in the window are automatically acked when the expire, i.e. when they fall out of the window after 
`windowLength + slidingInterval`. Note that the configuration `topology.message.timeout.secs` should be sufficiently more 
than `windowLength + slidingInterval` for time based windows; otherwise the tuples will timeout and get replayed and can result
in duplicate evaluations. For count based windows, the configuration should be adjusted such that `windowLength + slidingInterval`
tuples can be received within the timeout period.

## Example topology
An example toplogy `SlidingWindowTopology` shows how to use the apis to compute a sliding window sum and a tumbling window 
average.

