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
    
    private String uid;
    private long totalCount;
    private final static Map<String, AtomicLong> totalEmitCount = new HashMap<String, AtomicLong>();
    
    SpoutOutputCollector _collector;
    private long eventId = 0;
    private long myCount;
    private int source;
    
    public TestEventLogSpout(long totalCount) {
        this.uid = UUID.randomUUID().toString();
        this.totalCount = totalCount;
        
        synchronized (totalEmitCount) {
            if (null == totalEmitCount.get(uid)) {
                totalEmitCount.put(uid, new AtomicLong(0));
            }
            
        }
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
        synchronized(totalEmitCount) {            
            totalEmitCount.remove(uid);
        }
    }
    
    public boolean completed() {
        Long totalEmitted = totalEmitCount.get(uid).get();
        
        if (totalEmitted >= totalCount) {
            return true;
        }
        return false;
    }
        
    public void nextTuple() {
        if (eventId < myCount) { 
            eventId++;
            _collector.emit(new Values(source, eventId), eventId);
            totalEmitCount.get(uid).incrementAndGet();
        }        
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source", "eventId"));
    }
}