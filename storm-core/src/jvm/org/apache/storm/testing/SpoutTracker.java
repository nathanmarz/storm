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

import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.RegisteredGlobalState;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class SpoutTracker extends BaseRichSpout {
    IRichSpout _delegate;
    SpoutTrackOutputCollector _tracker;
    String _trackId;


    private class SpoutTrackOutputCollector implements ISpoutOutputCollector {
        public int transferred = 0;
        public int emitted = 0;
        public SpoutOutputCollector _collector;

        public SpoutTrackOutputCollector(SpoutOutputCollector collector) {
            _collector = collector;
        }
        
        private void recordSpoutEmit() {
            Map stats = (Map) RegisteredGlobalState.getState(_trackId);
            ((AtomicInteger) stats.get("spout-emitted")).incrementAndGet();
            
        }

        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            List<Integer> ret = _collector.emit(streamId, tuple, messageId);
            recordSpoutEmit();
            return ret;
        }

        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            _collector.emitDirect(taskId, streamId, tuple, messageId);
            recordSpoutEmit();
        }

        @Override
        public void reportError(Throwable error) {
        	_collector.reportError(error);
        }

        @Override
        public long getPendingCount() {
            return _collector.getPendingCount();
        }

    }


    public SpoutTracker(IRichSpout delegate, String trackId) {
        _delegate = delegate;
        _trackId = trackId;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _tracker = new SpoutTrackOutputCollector(collector);
        _delegate.open(conf, context, new SpoutOutputCollector(_tracker));
    }

    public void close() {
        _delegate.close();
    }

    public void nextTuple() {
        _delegate.nextTuple();
    }

    public void ack(Object msgId) {
        _delegate.ack(msgId);
        Map stats = (Map) RegisteredGlobalState.getState(_trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    public void fail(Object msgId) {
        _delegate.fail(msgId);
        Map stats = (Map) RegisteredGlobalState.getState(_trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }

}
