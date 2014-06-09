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
package backtype.storm.testing;

import static backtype.storm.utils.Utils.get;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestEventLogSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestEventLogSpout.class);
    
    private static final Map<String, Integer> acked = new HashMap<String, Integer>();
    private static final Map<String, Integer> failed = new HashMap<String, Integer>();
    
    private String uid;
    private long totalCount;
    
    SpoutOutputCollector _collector;
    private long eventId = 0;
    private long myCount;
    private int source;
    
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
    
    public TestEventLogSpout(long totalCount) {
        this.uid = UUID.randomUUID().toString();
        
        synchronized(acked) {
            acked.put(uid, 0);
        }
        synchronized(failed) {
            failed.put(uid, 0);
        }
        
        this.totalCount = totalCount;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        this.source = context.getThisTaskId();
        long taskCount = context.getComponentTasks(context.getThisComponentId()).size();
        myCount = totalCount / taskCount;
    }
    
    public void close() {
        
    }
    
    public void cleanup() {
        synchronized(acked) {            
            acked.remove(uid);
        } 
        synchronized(failed) {            
            failed.remove(uid);
        }
    }
    
    public boolean completed() {
        
        int ackedAmt;
        int failedAmt;
        
        synchronized(acked) {
            ackedAmt = acked.get(uid);
        }
        synchronized(failed) {
            failedAmt = failed.get(uid);
        }
        int totalEmitted = ackedAmt + failedAmt;
        
        if (totalEmitted >= totalCount) {
            return true;
        }
        return false;
    }
        
    public void nextTuple() {
        if (eventId < myCount) { 
            eventId++;
            _collector.emit(new Values(source, eventId), eventId);
        }        
    }
    
    public void ack(Object msgId) {
        synchronized(acked) {
            int curr = get(acked, uid, 0);
            acked.put(uid, curr+1);
        }
    }

    public void fail(Object msgId) {
        synchronized(failed) {
            int curr = get(failed, uid, 0);
            failed.put(uid, curr+1);
        }
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source", "eventId"));
    }
}