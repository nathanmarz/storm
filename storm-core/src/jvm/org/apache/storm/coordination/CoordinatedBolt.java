/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.coordination;

import org.apache.storm.topology.FailedException;
import java.util.Map.Entry;
import org.apache.storm.tuple.Values;
import org.apache.storm.generated.GlobalStreamId;
import java.util.Collection;
import org.apache.storm.Constants;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.storm.utils.Utils.get;

/**
 * Coordination requires the request ids to be globally unique for awhile. This is so it doesn't get confused
 * in the case of retries.
 */
public class CoordinatedBolt implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(CoordinatedBolt.class);

    public static interface FinishedCallback {
        void finishedId(Object id);
    }

    public static interface TimeoutCallback {
        void timeoutId(Object id);
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

    public class CoordinatedOutputCollector implements IOutputCollector {
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
            Object id = tuple.getValue(0);
            synchronized(_tracked) {
                TrackingInfo track = _tracked.get(id);
                if (track != null)
                    track.receivedTuples++;
            }
            boolean failed = checkFinishId(tuple, TupleType.REGULAR);
            if(failed) {
                _delegate.fail(tuple);                
            } else {
                _delegate.ack(tuple);
            }
        }

        public void fail(Tuple tuple) {
            Object id = tuple.getValue(0);
            synchronized(_tracked) {
                TrackingInfo track = _tracked.get(id);
                if (track != null)
                    track.failed = true;
            }
            checkFinishId(tuple, TupleType.REGULAR);
            _delegate.fail(tuple);
        }

        public void resetTimeout(Tuple tuple) {
            _delegate.resetTimeout(tuple);
        }
        
        public void reportError(Throwable error) {
            _delegate.reportError(error);
        }


        private void updateTaskCounts(Object id, List<Integer> tasks) {
            synchronized(_tracked) {
                TrackingInfo track = _tracked.get(id);
                if (track != null) {
                    Map<Integer, Integer> taskEmittedTuples = track.taskEmittedTuples;
                    for(Integer task: tasks) {
                        int newCount = get(taskEmittedTuples, task, 0) + 1;
                        taskEmittedTuples.put(task, newCount);
                    }
                }
            }
        }
    }

    private Map<String, SourceArgs> _sourceArgs;
    private IdStreamSpec _idStreamSpec;
    private IRichBolt _delegate;
    private Integer _numSourceReports;
    private List<Integer> _countOutTasks = new ArrayList<>();
    private OutputCollector _collector;
    private TimeCacheMap<Object, TrackingInfo> _tracked;

    public static class TrackingInfo {
        int reportCount = 0;
        int expectedTupleCount = 0;
        int receivedTuples = 0;
        boolean failed = false;
        Map<Integer, Integer> taskEmittedTuples = new HashMap<>();
        boolean receivedId = false;
        boolean finished = false;
        List<Tuple> ackTuples = new ArrayList<>();
        
        @Override
        public String toString() {
            return "reportCount: " + reportCount + "\n" +
                   "expectedTupleCount: " + expectedTupleCount + "\n" +
                   "receivedTuples: " + receivedTuples + "\n" +
                   "failed: " + failed + "\n" +
                   taskEmittedTuples.toString();
        }
    }

    
    public static class IdStreamSpec implements Serializable {
        GlobalStreamId _id;
        
        public GlobalStreamId getGlobalStreamId() {
            return _id;
        }

        public static IdStreamSpec makeDetectSpec(String component, String stream) {
            return new IdStreamSpec(component, stream);
        }        
        
        protected IdStreamSpec(String component, String stream) {
            _id = new GlobalStreamId(component, stream);
        }
    }
    
    public CoordinatedBolt(IRichBolt delegate) {
        this(delegate, null, null);
    }

    public CoordinatedBolt(IRichBolt delegate, String sourceComponent, SourceArgs sourceArgs, IdStreamSpec idStreamSpec) {
        this(delegate, singleSourceArgs(sourceComponent, sourceArgs), idStreamSpec);
    }
    
    public CoordinatedBolt(IRichBolt delegate, Map<String, SourceArgs> sourceArgs, IdStreamSpec idStreamSpec) {
        _sourceArgs = sourceArgs;
        if(_sourceArgs==null) _sourceArgs = new HashMap<>();
        _delegate = delegate;
        _idStreamSpec = idStreamSpec;
    }
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        TimeCacheMap.ExpiredCallback<Object, TrackingInfo> callback = null;
        if(_delegate instanceof TimeoutCallback) {
            callback = new TimeoutItems();
        }
        _tracked = new TimeCacheMap<>(context.maxTopologyMessageTimeout(), callback);
        _collector = collector;
        _delegate.prepare(config, context, new OutputCollector(new CoordinatedOutputCollector(collector)));
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

    private boolean checkFinishId(Tuple tup, TupleType type) {
        Object id = tup.getValue(0);
        boolean failed = false;
        
        synchronized(_tracked) {
            TrackingInfo track = _tracked.get(id);
            try {
                if(track!=null) {
                    boolean delayed = false;
                    if(_idStreamSpec==null && type == TupleType.COORD || _idStreamSpec!=null && type==TupleType.ID) {
                        track.ackTuples.add(tup);
                        delayed = true;
                    }
                    if(track.failed) {
                        failed = true;
                        for(Tuple t: track.ackTuples) {
                            _collector.fail(t);
                        }
                        _tracked.remove(id);
                    } else if(track.receivedId
                             && (_sourceArgs.isEmpty() ||
                                  track.reportCount==_numSourceReports &&
                                  track.expectedTupleCount == track.receivedTuples)){
                        if(_delegate instanceof FinishedCallback) {
                            ((FinishedCallback)_delegate).finishedId(id);
                        }
                        if(!(_sourceArgs.isEmpty() || type!=TupleType.REGULAR)) {
                            throw new IllegalStateException("Coordination condition met on a non-coordinating tuple. Should be impossible");
                        }
                        Iterator<Integer> outTasks = _countOutTasks.iterator();
                        while(outTasks.hasNext()) {
                            int task = outTasks.next();
                            int numTuples = get(track.taskEmittedTuples, task, 0);
                            _collector.emitDirect(task, Constants.COORDINATED_STREAM_ID, tup, new Values(id, numTuples));
                        }
                        for(Tuple t: track.ackTuples) {
                            _collector.ack(t);
                        }
                        track.finished = true;
                        _tracked.remove(id);
                    }
                    if(!delayed && type!=TupleType.REGULAR) {
                        if(track.failed) {
                            _collector.fail(tup);
                        } else {
                            _collector.ack(tup);                            
                        }
                    }
                } else {
                    if(type!=TupleType.REGULAR) _collector.fail(tup);
                }
            } catch(FailedException e) {
                LOG.error("Failed to finish batch", e);
                for(Tuple t: track.ackTuples) {
                    _collector.fail(t);
                }
                _tracked.remove(id);
                failed = true;
            }
        }
        return failed;
    }

    public void execute(Tuple tuple) {
        Object id = tuple.getValue(0);
        TrackingInfo track;
        TupleType type = getTupleType(tuple);
        synchronized(_tracked) {
            track = _tracked.get(id);
            if(track==null) {
                track = new TrackingInfo();
                if(_idStreamSpec==null) track.receivedId = true;
                _tracked.put(id, track);
            }
        }
        
        if(type==TupleType.ID) {
            synchronized(_tracked) {
                track.receivedId = true;
            }
            checkFinishId(tuple, type);            
        } else if(type==TupleType.COORD) {
            int count = (Integer) tuple.getValue(1);
            synchronized(_tracked) {
                track.reportCount++;
                track.expectedTupleCount+=count;
            }
            checkFinishId(tuple, type);
        } else {            
            synchronized(_tracked) {
                _delegate.execute(tuple);
            }
        }
    }

    public void cleanup() {
        _delegate.cleanup();
        _tracked.cleanup();
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
        Map<String, SourceArgs> ret = new HashMap<>();
        ret.put(sourceComponent, sourceArgs);
        return ret;
    }
    
    private class TimeoutItems implements TimeCacheMap.ExpiredCallback<Object, TrackingInfo> {
        @Override
        public void expire(Object id, TrackingInfo val) {
            synchronized(_tracked) {
                // the combination of the lock and the finished flag ensure that
                // an id is never timed out if it has been finished
                val.failed = true;
                if(!val.finished) {
                    ((TimeoutCallback) _delegate).timeoutId(id);
                }
            }
        }
    }
    
    private TupleType getTupleType(Tuple tuple) {
        if(_idStreamSpec!=null
                && tuple.getSourceGlobalStreamId().equals(_idStreamSpec._id)) {
            return TupleType.ID;
        } else if(!_sourceArgs.isEmpty()
                && tuple.getSourceStreamId().equals(Constants.COORDINATED_STREAM_ID)) {
            return TupleType.COORD;
        } else {
            return TupleType.REGULAR;
        }
    }
    
    static enum TupleType {
        REGULAR,
        ID,
        COORD
    }
}
