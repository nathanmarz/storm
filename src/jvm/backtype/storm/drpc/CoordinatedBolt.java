package backtype.storm.drpc;

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
import static backtype.storm.utils.Utils.tuple;


public class CoordinatedBolt implements IRichBolt {
    public static Logger LOG = Logger.getLogger(CoordinatedBolt.class);

    public static interface FinishedCallback {
        void finishedId(Object id);
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
    }

    public class CoordinatedOutputCollector extends OutputCollector {
        IOutputCollector _delegate;

        public CoordinatedOutputCollector(IOutputCollector delegate) {
            _delegate = delegate;
        }

        public List<Integer> emit(String stream, Collection<Tuple> anchors, List<Object> tuple) {
            List<Integer> tasks = _delegate.emit(stream, anchors, tuple);
            updateTaskCounts(tuple.get(0), tasks);
            return tasks;
        }

        public void emitDirect(int task, String stream, Collection<Tuple> anchors, List<Object> tuple) {
            updateTaskCounts(tuple.get(0), Arrays.asList(task));
            _delegate.emitDirect(task, stream, anchors, tuple);
        }

        public void ack(Tuple tuple) {
            _delegate.ack(tuple);
            Object id = tuple.getValue(0);
            synchronized(_tracked) {
                _tracked.get(id).receivedTuples++;
            }
            checkFinishId(id);
        }

        public void fail(Tuple tuple) {
            _delegate.fail(tuple);
            Object id = tuple.getValue(0);
            synchronized(_tracked) {
                _tracked.get(id).receivedTuples++;
            }
            checkFinishId(id);
        }
        
        public void reportError(Throwable error) {
            _delegate.reportError(error);
        }


        private void updateTaskCounts(Object id, List<Integer> tasks) {
            Map<Integer, Integer> taskEmittedTuples = _tracked.get(id).taskEmittedTuples;
            for(Integer task: tasks) {
                int newCount = get(taskEmittedTuples, task, 0) + 1;
                taskEmittedTuples.put(task, newCount);
            }
        }
    }

    private SourceArgs _sourceArgs;
    private String _idComponent;
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

    public CoordinatedBolt(IRichBolt delegate, SourceArgs sourceArgs, String idComponent) {
        _sourceArgs = sourceArgs;
        _delegate = delegate;
        _idComponent = idComponent;
    }

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        _tracked = new TimeCacheMap<Object, TrackingInfo>(Utils.toInteger(config.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
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
        if(_sourceArgs!=null) {
            if(_sourceArgs.singleCount) {
                _numSourceReports = 1;
            } else {
                String sourceComponent = context.getThisSources().keySet().iterator().next().get_componentId();
                _numSourceReports = context.getComponentTasks(sourceComponent).size();
            }
        }
    }

    private void checkFinishId(Object id) {
        synchronized(_tracked) {
            TrackingInfo track = _tracked.get(id);
            if(track!=null
                    && track.receivedId 
                    && (_sourceArgs==null
                        ||
                       track.reportCount==_numSourceReports &&
                       track.expectedTupleCount == track.receivedTuples)) {
                if(_delegate instanceof FinishedCallback) {
                    ((FinishedCallback)_delegate).finishedId(id);
                }
                Iterator<Integer> outTasks = _countOutTasks.iterator();
                while(outTasks.hasNext()) {
                    int task = outTasks.next();
                    int numTuples = get(track.taskEmittedTuples, task, 0);
                    _collector.emitDirect(task, Constants.COORDINATED_STREAM_ID, tuple(id, numTuples));
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
                if(_idComponent==null) track.receivedId = true;
                _tracked.put(id, track);
            }
        }
        
        boolean checkFinish = false;
        if(_idComponent!=null
                && tuple.getSourceComponent().equals(_idComponent)
                && tuple.getSourceStreamId().equals(PrepareRequest.ID_STREAM)) {
            synchronized(_tracked) {
                track.receivedId = true;
            }
            checkFinish = true;
        } else if(_sourceArgs!=null
                && tuple.getSourceStreamId().equals(Constants.COORDINATED_STREAM_ID)) {
            int count = (Integer) tuple.getValue(1);
            synchronized(_tracked) {
                track.reportCount++;
                track.expectedTupleCount+=count;
            }
            checkFinish = true;
        } else {            
            _delegate.execute(tuple);
        }
        if(checkFinish) {
            checkFinishId(id);
        }
    }

    public void cleanup() {
        _delegate.cleanup();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
        declarer.declareStream(Constants.COORDINATED_STREAM_ID, true, new Fields("id", "count"));
    }

}
