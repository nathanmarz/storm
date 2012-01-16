package backtype.storm.coordination;

import java.util.Map.Entry;
import backtype.storm.tuple.IAnchorable;
import backtype.storm.tuple.Values;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.Config;
import java.util.Collection;
import backtype.storm.Constants;
import backtype.storm.generated.Grouping;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import static backtype.storm.utils.Utils.get;

/**
 * Coordination requires the request ids to be globally unique for awhile. This is so it doesn't get confused
 * in the case of retries.
 */
public class CoordinatedBolt implements IRichBolt {
    public static Logger LOG = Logger.getLogger(CoordinatedBolt.class);

    public static interface FinishedCallback {
        void finishedId(FinishedTuple id);
    }

    public static class SourceArgs implements Serializable {
        public boolean singleCount;

        protected SourceArgs(boolean singleCount) {
            this.singleCount = singleCount;
        }

        public static SourceArgs single() {
            return new SourceArgs(true);
        }

        public static SourceArgs all() {
            return new SourceArgs(false);
        }
        
        @Override
        public String toString() {
            return "<Single: " + singleCount + ">";
        }
    }

    public class CoordinatedOutputCollector extends OutputCollector {
        IOutputCollector _delegate;

        public CoordinatedOutputCollector(IOutputCollector delegate) {
            _delegate = delegate;
        }

        public List<Integer> emit(String stream, Collection<IAnchorable> anchors, List<Object> tuple) {
            List<Integer> tasks = _delegate.emit(stream, anchors, tuple);
            updateTaskCounts(tuple.get(0), tasks);
            return tasks;
        }

        public void emitDirect(int task, String stream, Collection<IAnchorable> anchors, List<Object> tuple) {
            updateTaskCounts(tuple.get(0), Arrays.asList(task));
            _delegate.emitDirect(task, stream, anchors, tuple);
        }

        public void ack(Tuple tuple) {
            Object id = tuple.getValue(0);
            synchronized(_tracked) {
                _tracked.get(id).receivedTuples++;
            }
            checkFinishId(tuple);
            _delegate.ack(tuple);
        }

        public void fail(Tuple tuple) {
            // don't increment here... don't want to complete in case of failure
            _delegate.fail(tuple);
        }
        
        public void reportError(Throwable error) {
            _delegate.reportError(error);
        }


        private void updateTaskCounts(Object id, List<Integer> tasks) {
            synchronized(_tracked) {
                Map<Integer, Integer> taskEmittedTuples = _tracked.get(id).taskEmittedTuples;
                for(Integer task: tasks) {
                    int newCount = get(taskEmittedTuples, task, 0) + 1;
                    taskEmittedTuples.put(task, newCount);
                }
            }
        }
    }

    private Map<String, SourceArgs> _sourceArgs;
    private GlobalStreamId _idSource;
    private IRichBolt _delegate;
    private Integer _numSourceReports;
    private List<Integer> _countOutTasks = new ArrayList<Integer>();;
    private OutputCollector _collector;
    private TimeCacheMap<Object, TrackingInfo> _tracked;

    public static class TrackingInfo {
        int reportCount = 0;
        int expectedTupleCount = 0;
        int receivedTuples = 0;
        Map<Integer, Integer> taskEmittedTuples = new HashMap<Integer, Integer>();
        boolean receivedId = false;
        
        @Override
        public String toString() {
            return "reportCount: " + reportCount + "\n" +
                   "expectedTupleCount: " + expectedTupleCount + "\n" +
                   "receivedTuples: " + receivedTuples + "\n" +
                   taskEmittedTuples.toString();
        }
    }

    
    public CoordinatedBolt(IRichBolt delegate) {
        this(delegate, null, null);
    }

    public CoordinatedBolt(IRichBolt delegate, String sourceComponent, SourceArgs sourceArgs, GlobalStreamId idSource) {
        this(delegate, singleSourceArgs(sourceComponent, sourceArgs), idSource);
    }
    
    public CoordinatedBolt(IRichBolt delegate, Map<String, SourceArgs> sourceArgs, GlobalStreamId idSource) {
        _sourceArgs = sourceArgs;
        if(_sourceArgs==null) _sourceArgs = new HashMap<String, SourceArgs>();
        _delegate = delegate;
        _idSource = idSource;
    }
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        _tracked = new TimeCacheMap<Object, TrackingInfo>(Utils.getInt(config.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
        _collector = collector;
        _delegate.prepare(config, context, new CoordinatedOutputCollector(collector));
        for(String component: Utils.get(context.getThisTargets(),
                                        Constants.COORDINATED_STREAM_ID,
                                        new HashMap<String, Grouping>())
                                        .keySet()) {
            for(Integer task: context.getComponentTasks(component)) {
                _countOutTasks.add(task);
            }
        }
        if(!_sourceArgs.isEmpty()) {
            _numSourceReports = 0;
            for(Entry<String, SourceArgs> entry: _sourceArgs.entrySet()) {
                if(entry.getValue().singleCount) {
                    _numSourceReports+=1;
                } else {
                    _numSourceReports+=context.getComponentTasks(entry.getKey()).size();
                }
            }
        }
    }

    private void checkFinishId(Tuple tup) {
        Object id = tup.getValue(0);
        synchronized(_tracked) {
            TrackingInfo track = _tracked.get(id);
            if(track!=null
                    && track.receivedId 
                    && (_sourceArgs.isEmpty()
                        ||
                       track.reportCount==_numSourceReports &&
                       track.expectedTupleCount == track.receivedTuples)) {
                if(_delegate instanceof FinishedCallback) {
                    ((FinishedCallback)_delegate).finishedId(new FinishedTupleImpl(tup));
                }
                if(!_sourceArgs.isEmpty() &&
                        !tup.getSourceStreamId().equals(Constants.COORDINATED_STREAM_ID)) {
                    throw new IllegalStateException("Coordination condition met on a non-coordinating tuple. Should be impossible");
                }
                Iterator<Integer> outTasks = _countOutTasks.iterator();
                while(outTasks.hasNext()) {
                    int task = outTasks.next();
                    int numTuples = get(track.taskEmittedTuples, task, 0);
                    _collector.emitDirect(task, Constants.COORDINATED_STREAM_ID, tup, new Values(id, numTuples));
                }
                _tracked.remove(id);
            }
        }
    }

    public void execute(Tuple tuple) {
        Object id = tuple.getValue(0);
        TrackingInfo track;
        synchronized(_tracked) {
            track = _tracked.get(id);
            if(track==null) {
                track = new TrackingInfo();
                if(_idSource==null) track.receivedId = true;
                _tracked.put(id, track);
            }
        }
        
        if(_idSource!=null
                && tuple.getSourceComponent().equals(_idSource.get_componentId())
                && tuple.getSourceStreamId().equals(_idSource.get_streamId())) {
            synchronized(_tracked) {
                track.receivedId = true;
            }
            checkFinishId(tuple);
            _collector.ack(tuple);
        } else if(!_sourceArgs.isEmpty()
                && tuple.getSourceStreamId().equals(Constants.COORDINATED_STREAM_ID)) {
            int count = (Integer) tuple.getValue(1);
            synchronized(_tracked) {
                track.reportCount++;
                track.expectedTupleCount+=count;
            }
            checkFinishId(tuple);
            _collector.ack(tuple);
        } else {            
            _delegate.execute(tuple);
        }
    }

    public void cleanup() {
        _delegate.cleanup();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
        declarer.declareStream(Constants.COORDINATED_STREAM_ID, true, new Fields("id", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _delegate.getComponentConfiguration();
    }
    
    private static Map<String, SourceArgs> singleSourceArgs(String sourceComponent, SourceArgs sourceArgs) {
        Map<String, SourceArgs> ret = new HashMap<String, SourceArgs>();
        ret.put(sourceComponent, sourceArgs);
        return ret;
    }
}
