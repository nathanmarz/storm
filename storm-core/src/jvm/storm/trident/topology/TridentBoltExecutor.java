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
package storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.BatchOutputCollectorImpl;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import storm.trident.spout.IBatchID;

public class TridentBoltExecutor implements IRichBolt {
    public static String COORD_STREAM_PREFIX = "$coord-";
    
    public static String COORD_STREAM(String batch) {
        return COORD_STREAM_PREFIX + batch;
    }
    
    public static class CoordType implements Serializable {
        public boolean singleCount;
        
        protected CoordType(boolean singleCount) {
            this.singleCount = singleCount;
        }

        public static CoordType single() {
            return new CoordType(true);
        }

        public static CoordType all() {
            return new CoordType(false);
        }

        @Override
        public boolean equals(Object o) {
            return singleCount == ((CoordType) o).singleCount;
        }        
        
        @Override
        public String toString() {
            return "<Single: " + singleCount + ">";
        }
    }
    
    public static class CoordSpec implements Serializable {
        public GlobalStreamId commitStream = null;
        public Map<String, CoordType> coords = new HashMap<String, CoordType>();
        
        public CoordSpec() {
        }
    }
    
    public static class CoordCondition implements Serializable {
        public GlobalStreamId commitStream;
        public int expectedTaskReports;
        Set<Integer> targetTasks;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }        
    }
    
    Map<GlobalStreamId, String> _batchGroupIds;
    Map<String, CoordSpec> _coordSpecs;
    Map<String, CoordCondition> _coordConditions;
    ITridentBatchBolt _bolt;
    long _messageTimeoutMs;
    long _lastRotate;
    
    RotatingMap _batches;
    
    // map from batchgroupid to coordspec
    public TridentBoltExecutor(ITridentBatchBolt bolt, Map<GlobalStreamId, String> batchGroupIds, Map<String, CoordSpec> coordinationSpecs) {
        _batchGroupIds = batchGroupIds;
        _coordSpecs = coordinationSpecs;
        _bolt = bolt;        
    }
    
    public static class TrackedBatch {
        int attemptId;
        BatchInfo info;
        CoordCondition condition;
        int reportedTasks = 0;
        int expectedTupleCount = 0;
        int receivedTuples = 0;
        Map<Integer, Integer> taskEmittedTuples = new HashMap();
        boolean failed = false;
        boolean receivedCommit;
        Tuple delayedAck = null;
        
        public TrackedBatch(BatchInfo info, CoordCondition condition, int attemptId) {
            this.info = info;
            this.condition = condition;
            this.attemptId = attemptId;
            receivedCommit = condition.commitStream == null;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }        
    }
    
    public class CoordinatedOutputCollector implements IOutputCollector {
        IOutputCollector _delegate;
        
        TrackedBatch _currBatch = null;;
        
        public void setCurrBatch(TrackedBatch batch) {
            _currBatch = batch;
        }
        
        public CoordinatedOutputCollector(IOutputCollector delegate) {
            _delegate = delegate;
        }

        public List<Integer> emit(String stream, Collection<Tuple> anchors, List<Object> tuple) {
            List<Integer> tasks = _delegate.emit(stream, anchors, tuple);
            updateTaskCounts(tasks);
            return tasks;
        }

        public void emitDirect(int task, String stream, Collection<Tuple> anchors, List<Object> tuple) {
            updateTaskCounts(Arrays.asList(task));
            _delegate.emitDirect(task, stream, anchors, tuple);
        }

        public void ack(Tuple tuple) {
            throw new IllegalStateException("Method should never be called");
        }

        public void fail(Tuple tuple) {
            throw new IllegalStateException("Method should never be called");
        }
        
        public void reportError(Throwable error) {
            _delegate.reportError(error);
        }


        private void updateTaskCounts(List<Integer> tasks) {
            if(_currBatch!=null) {
                Map<Integer, Integer> taskEmittedTuples = _currBatch.taskEmittedTuples;
                for(Integer task: tasks) {
                    int newCount = Utils.get(taskEmittedTuples, task, 0) + 1;
                    taskEmittedTuples.put(task, newCount);
                }
            }
        }
    }
    
    OutputCollector _collector;
    CoordinatedOutputCollector _coordCollector;
    BatchOutputCollector _coordOutputCollector;
    TopologyContext _context;
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {        
        _messageTimeoutMs = context.maxTopologyMessageTimeout() * 1000L;
        _lastRotate = System.currentTimeMillis();
        _batches = new RotatingMap(2);
        _context = context;
        _collector = collector;
        _coordCollector = new CoordinatedOutputCollector(collector);
        _coordOutputCollector = new BatchOutputCollectorImpl(new OutputCollector(_coordCollector));
                
        _coordConditions = (Map) context.getExecutorData("__coordConditions");
        if(_coordConditions==null) {
            _coordConditions = new HashMap();
            for(String batchGroup: _coordSpecs.keySet()) {
                CoordSpec spec = _coordSpecs.get(batchGroup);
                CoordCondition cond = new CoordCondition();
                cond.commitStream = spec.commitStream;
                cond.expectedTaskReports = 0;
                for(String comp: spec.coords.keySet()) {
                    CoordType ct = spec.coords.get(comp);
                    if(ct.equals(CoordType.single())) {
                        cond.expectedTaskReports+=1;
                    } else {
                        cond.expectedTaskReports+=context.getComponentTasks(comp).size();
                    }
                }
                cond.targetTasks = new HashSet<Integer>();
                for(String component: Utils.get(context.getThisTargets(),
                                        COORD_STREAM(batchGroup),
                                        new HashMap<String, Grouping>()).keySet()) {
                    cond.targetTasks.addAll(context.getComponentTasks(component));
                }
                _coordConditions.put(batchGroup, cond);
            }
            context.setExecutorData("_coordConditions", _coordConditions);
        }
        _bolt.prepare(conf, context, _coordOutputCollector);
    }
    
    private void failBatch(TrackedBatch tracked, FailedException e) {
        if(e!=null && e instanceof ReportedFailedException) {
            _collector.reportError(e);
        }
        tracked.failed = true;
        if(tracked.delayedAck!=null) {
            _collector.fail(tracked.delayedAck);
            tracked.delayedAck = null;
        }
    }
    
    private void failBatch(TrackedBatch tracked) {
        failBatch(tracked, null);
    }

    private boolean finishBatch(TrackedBatch tracked, Tuple finishTuple) {
        boolean success = true;
        try {
            _bolt.finishBatch(tracked.info);
            String stream = COORD_STREAM(tracked.info.batchGroup);
            for(Integer task: tracked.condition.targetTasks) {
                _collector.emitDirect(task, stream, finishTuple, new Values(tracked.info.batchId, Utils.get(tracked.taskEmittedTuples, task, 0)));
            }
            if(tracked.delayedAck!=null) {
                _collector.ack(tracked.delayedAck);
                tracked.delayedAck = null;
            }
        } catch(FailedException e) {
            failBatch(tracked, e);
            success = false;
        }
        _batches.remove(tracked.info.batchId.getId());
        return success;
    }
    
    private void checkFinish(TrackedBatch tracked, Tuple tuple, TupleType type) {
        if(tracked.failed) {
            failBatch(tracked);
            _collector.fail(tuple);
            return;
        }
        CoordCondition cond = tracked.condition;
        boolean delayed = tracked.delayedAck==null &&
                              (cond.commitStream!=null && type==TupleType.COMMIT
                               || cond.commitStream==null);
        if(delayed) {
            tracked.delayedAck = tuple;
        }
        boolean failed = false;
        if(tracked.receivedCommit && tracked.reportedTasks == cond.expectedTaskReports) {
            if(tracked.receivedTuples == tracked.expectedTupleCount) {
                finishBatch(tracked, tuple);                
            } else {
                //TODO: add logging that not all tuples were received
                failBatch(tracked);
                _collector.fail(tuple);
                failed = true;
            }
        }
        
        if(!delayed && !failed) {
            _collector.ack(tuple);
        }
        
    }
    
    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            long now = System.currentTimeMillis();
            if(now - _lastRotate > _messageTimeoutMs) {
                _batches.rotate();
                _lastRotate = now;
            }
            return;
        }
        String batchGroup = _batchGroupIds.get(tuple.getSourceGlobalStreamid());
        if(batchGroup==null) {
            // this is so we can do things like have simple DRPC that doesn't need to use batch processing
            _coordCollector.setCurrBatch(null);
            _bolt.execute(null, tuple);
            _collector.ack(tuple);
            return;
        }
        IBatchID id = (IBatchID) tuple.getValue(0);
        //get transaction id
        //if it already exissts and attempt id is greater than the attempt there
        
        
        TrackedBatch tracked = (TrackedBatch) _batches.get(id.getId());
//        if(_batches.size() > 10 && _context.getThisTaskIndex() == 0) {
//            System.out.println("Received in " + _context.getThisComponentId() + " " + _context.getThisTaskIndex()
//                    + " (" + _batches.size() + ")" +
//                    "\ntuple: " + tuple +
//                    "\nwith tracked " + tracked +
//                    "\nwith id " + id + 
//                    "\nwith group " + batchGroup
//                    + "\n");
//            
//        }
        //System.out.println("Num tracked: " + _batches.size() + " " + _context.getThisComponentId() + " " + _context.getThisTaskIndex());
        
        // this code here ensures that only one attempt is ever tracked for a batch, so when
        // failures happen you don't get an explosion in memory usage in the tasks
        if(tracked!=null) {
            if(id.getAttemptId() > tracked.attemptId) {
                _batches.remove(id.getId());
                tracked = null;
            } else if(id.getAttemptId() < tracked.attemptId) {
                // no reason to try to execute a previous attempt than we've already seen
                return;
            }
        }
        
        if(tracked==null) {
            tracked = new TrackedBatch(new BatchInfo(batchGroup, id, _bolt.initBatchState(batchGroup, id)), _coordConditions.get(batchGroup), id.getAttemptId());
            _batches.put(id.getId(), tracked);
        }
        _coordCollector.setCurrBatch(tracked);
        
        //System.out.println("TRACKED: " + tracked + " " + tuple);
        
        TupleType t = getTupleType(tuple, tracked);
        if(t==TupleType.COMMIT) {
            tracked.receivedCommit = true;
            checkFinish(tracked, tuple, t);
        } else if(t==TupleType.COORD) {
            int count = tuple.getInteger(1);
            tracked.reportedTasks++;
            tracked.expectedTupleCount+=count;
            checkFinish(tracked, tuple, t);
        } else {
            tracked.receivedTuples++;
            boolean success = true;
            try {
                _bolt.execute(tracked.info, tuple);
                if(tracked.condition.expectedTaskReports==0) {
                    success = finishBatch(tracked, tuple);
                }
            } catch(FailedException e) {
                failBatch(tracked, e);
            }
            if(success) {
                _collector.ack(tuple);                   
            } else {
                _collector.fail(tuple);
            }
        }
        _coordCollector.setCurrBatch(null);
    }

    @Override
    public void cleanup() {
        _bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
        for(String batchGroup: _coordSpecs.keySet()) {
            declarer.declareStream(COORD_STREAM(batchGroup), true, new Fields("id", "count"));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = _bolt.getComponentConfiguration();
        if(ret==null) ret = new HashMap();
        ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        // TODO: Need to be able to set the tick tuple time to the message timeout, ideally without parameterization
        return ret;
    }
    
    private TupleType getTupleType(Tuple tuple, TrackedBatch batch) {
        CoordCondition cond = batch.condition;
        if(cond.commitStream!=null
                && tuple.getSourceGlobalStreamid().equals(cond.commitStream)) {
            return TupleType.COMMIT;
        } else if(cond.expectedTaskReports > 0
                && tuple.getSourceStreamId().startsWith(COORD_STREAM_PREFIX)) {
            return TupleType.COORD;
        } else {
            return TupleType.REGULAR;
        }
    }
    
    static enum TupleType {
        REGULAR,
        COMMIT,
        COORD
    }    
}
