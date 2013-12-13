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
package storm.trident.spout;

import backtype.storm.Config;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import storm.trident.topology.TridentBoltExecutor;
import storm.trident.tuple.ConsList;
import storm.trident.util.TridentUtils;


public class RichSpoutBatchTriggerer implements IRichSpout {

    String _stream;
    IRichSpout _delegate;
    List<Integer> _outputTasks;
    Random _rand;
    String _coordStream;
    
    public RichSpoutBatchTriggerer(IRichSpout delegate, String streamName, String batchGroup) {
        _delegate = delegate;
        _stream = streamName;
        _coordStream = TridentBoltExecutor.COORD_STREAM(batchGroup);
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _delegate.open(conf, context, new SpoutOutputCollector(new StreamOverrideCollector(collector)));
        _outputTasks = new ArrayList<Integer>();
        for(String component: Utils.get(context.getThisTargets(),
                                        _coordStream,
                                        new HashMap<String, Grouping>()).keySet()) {
            _outputTasks.addAll(context.getComponentTasks(component));
        }
        _rand = new Random(Utils.secureRandomLong());
    }

    @Override
    public void close() {
        _delegate.close();
    }

    @Override
    public void activate() {
        _delegate.activate();
    }

    @Override
    public void deactivate() {
        _delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        _delegate.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        Long batchId = _msgIdToBatchId.remove((Long) msgId);
        FinishCondition cond = _finishConditions.get(batchId);
        if(cond!=null) {
            cond.vals.remove((Long) msgId);
            if(cond.vals.isEmpty()) {
                _finishConditions.remove(batchId);
                _delegate.ack(cond.msgId);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        Long batchId = _msgIdToBatchId.remove((Long) msgId);
        FinishCondition cond = _finishConditions.remove(batchId);
        if(cond!=null) {
            _delegate.fail(cond.msgId);            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outFields = TridentUtils.getSingleOutputStreamFields(_delegate);
        outFields = TridentUtils.fieldsConcat(new Fields("$id$"), outFields);
        declarer.declareStream(_stream, outFields);
        // try to find a way to merge this code with what's already done in TridentBoltExecutor
        declarer.declareStream(_coordStream, true, new Fields("id", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = _delegate.getComponentConfiguration();
        if(conf==null) conf = new HashMap();
        else conf = new HashMap(conf);
        Config.registerSerialization(conf, RichSpoutBatchId.class, RichSpoutBatchIdSerializer.class);
        return conf;
    }
    
    static class FinishCondition {
        Set<Long> vals = new HashSet<Long>();
        Object msgId;
    }
    
    Map<Long, Long> _msgIdToBatchId = new HashMap();
    
    Map<Long, FinishCondition> _finishConditions = new HashMap();
    
    class StreamOverrideCollector implements ISpoutOutputCollector {
        
        SpoutOutputCollector _collector;
        
        public StreamOverrideCollector(SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public List<Integer> emit(String ignore, List<Object> values, Object msgId) {
            long batchIdVal = _rand.nextLong();
            Object batchId = new RichSpoutBatchId(batchIdVal);
            FinishCondition finish = new FinishCondition();
            finish.msgId = msgId;
            List<Integer> tasks = _collector.emit(_stream, new ConsList(batchId, values));
            Set<Integer> outTasksSet = new HashSet<Integer>(tasks);
            for(Integer t: _outputTasks) {
                int count = 0;
                if(outTasksSet.contains(t)) {
                    count = 1;
                }
                long r = _rand.nextLong();
                _collector.emitDirect(t, _coordStream, new Values(batchId, count), r);
                finish.vals.add(r);
            }
            _finishConditions.put(batchIdVal, finish);
            return tasks;
        }

        @Override
        public void emitDirect(int task, String ignore, List<Object> values, Object msgId) {
            throw new RuntimeException("Trident does not support direct emits from spouts");
        }

        @Override
        public void reportError(Throwable t) {
            _collector.reportError(t);
        }
        
    }
}
