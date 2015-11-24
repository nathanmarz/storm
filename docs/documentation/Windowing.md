# Windowing support in core storm

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

