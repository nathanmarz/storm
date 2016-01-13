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
package org.apache.storm.testing;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.apache.storm.utils.Utils.get;

public class FixedTupleSpout implements IRichSpout {
    private static final Map<String, Integer> acked = new HashMap<String, Integer>();
    private static final Map<String, Integer> failed = new HashMap<String, Integer>();

    public static int getNumAcked(String stormId) {
        synchronized(acked) {
            return get(acked, stormId, 0);
        }
    }

    public static int getNumFailed(String stormId) {
        synchronized(failed) {
            return get(failed, stormId, 0);
        }
    }
    
    public static void clear(String stormId) {
        acked.remove(stormId);
        failed.remove(stormId);
    }

    private List<FixedTuple> _tuples;
    private SpoutOutputCollector _collector;

    private TopologyContext _context;
    private List<FixedTuple> _serveTuples;
    private Map<String, FixedTuple> _pending;

    private String _id;
    private String _fieldName;

    public FixedTupleSpout(List tuples) {
        this(tuples, null);
    }

    public FixedTupleSpout(List tuples, String fieldName) {
        _id = UUID.randomUUID().toString();
        synchronized(acked) {
            acked.put(_id, 0);
        }
        synchronized(failed) {
            failed.put(_id, 0);
        }
        _tuples = new ArrayList<FixedTuple>();
        for(Object o: tuples) {
            FixedTuple ft;
            if(o instanceof FixedTuple) {
                ft = (FixedTuple) o;
            } else {
                ft = new FixedTuple((List) o);
            }
            _tuples.add(ft);
        }
        _fieldName = fieldName;
    }

    public List<FixedTuple> getSourceTuples() {
        return _tuples;
    }
    
    public int getCompleted() {
        int ackedAmt;
        int failedAmt;
        
        synchronized(acked) {
            ackedAmt = acked.get(_id);
        }
        synchronized(failed) {
            failedAmt = failed.get(_id);
        }
        return ackedAmt + failedAmt;
    }
    
    public void cleanup() {
        synchronized(acked) {            
            acked.remove(_id);
        } 
        synchronized(failed) {            
            failed.remove(_id);
        }
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _context = context;
        List<Integer> tasks = context.getComponentTasks(context.getThisComponentId());
        int startIndex;
        for(startIndex=0; startIndex<tasks.size(); startIndex++) {
            if(tasks.get(startIndex)==context.getThisTaskId()) {
                break;
            }
        }
        _collector = collector;
        _pending = new HashMap<String, FixedTuple>();
        _serveTuples = new ArrayList<FixedTuple>();
        for(int i=startIndex; i<_tuples.size(); i+=tasks.size()) {
            _serveTuples.add(_tuples.get(i));
        }
    }

    public void close() {
    }

    public void nextTuple() {
        if(_serveTuples.size()>0) {
            FixedTuple ft = _serveTuples.remove(0);
            String id = UUID.randomUUID().toString();
            _pending.put(id, ft);
            _collector.emit(ft.stream, ft.values, id);
        } else {
            Utils.sleep(100);
        }
    }

    public void ack(Object msgId) {
        synchronized(acked) {
            int curr = get(acked, _id, 0);
            acked.put(_id, curr+1);
        }
    }

    public void fail(Object msgId) {
        synchronized(failed) {
            int curr = get(failed, _id, 0);
            failed.put(_id, curr+1);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
        if (_fieldName != null) {
            declarer.declare(new Fields(_fieldName));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
