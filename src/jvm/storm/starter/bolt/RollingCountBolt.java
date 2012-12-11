package storm.starter.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import storm.starter.tools.NthLastModifiedTimeTracker;
import storm.starter.tools.SlidingWindowCounter;
import storm.starter.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * 
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * 
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * 
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 * 
 */
public class RollingCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE = "Actual window length is %d seconds when it should be %d seconds"
        + " (you can safely ignore this warning during the startup phase)";

    private final SlidingWindowCounter<Object> counter;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCountBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.info("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
        }
        else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        emit(counts, actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
