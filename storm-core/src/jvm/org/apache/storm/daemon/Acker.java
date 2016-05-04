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
package org.apache.storm.daemon;

import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Acker implements IBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Acker.class);

    private static final long serialVersionUID = 4430906880683183091L;

    public static final String ACKER_COMPONENT_ID = "__acker";
    public static final String ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
    public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";
    public static final String ACKER_RESET_TIMEOUT_STREAM_ID = "__ack_reset_timeout";

    public static final int TIMEOUT_BUCKET_NUM = 3;

    private OutputCollector collector;
    private RotatingMap<Object, AckObject> pending;

    private class AckObject {
        public long val = 0L;
        public Integer spoutTask = null;
        public boolean failed = false;

        // val xor value
        public void updateAck(Long value) {
            val = Utils.bitXor(val, value);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new RotatingMap<Object, AckObject>(TIMEOUT_BUCKET_NUM);
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            Map<Object, AckObject> tmp = pending.rotate();
            LOG.debug("Number of timeout tuples:{}", tmp.size());
            return;
        }

        String streamId = input.getSourceStreamId();
        Object id = input.getValue(0);
        AckObject curr = pending.get(id);
        if (ACKER_INIT_STREAM_ID.equals(streamId)) {
            if (curr == null) {
                curr = new AckObject();
                curr.val = input.getLong(1);
                curr.spoutTask = input.getInteger(2);
                pending.put(id, curr);
            } else {
                // If receiving bolt's ack before the init message from spout, just update the xor value.
                curr.updateAck(input.getLong(1));
                curr.spoutTask = input.getInteger(2);
            }
        } else if (ACKER_ACK_STREAM_ID.equals(streamId)) {
            if (curr != null) {
                curr.updateAck(input.getLong(1));
            } else {
                curr = new AckObject();
                curr.val = input.getLong(1);
                pending.put(id, curr);
            }
        } else if (ACKER_FAIL_STREAM_ID.equals(streamId)) {
            // For the case that ack_fail message arrives before ack_init
            if (curr == null) {
                curr = new AckObject();
            }
            curr.failed = true;
            pending.put(id, curr);
        } else if (ACKER_RESET_TIMEOUT_STREAM_ID.equals(streamId)) {
            if (curr == null) {
                curr = new AckObject();
            }
            pending.put(id, curr);
        } else {
            LOG.warn("Unknown source stream {} from task-{}", streamId, input.getSourceTask());
            return;
        }

        Integer task = curr.spoutTask;
        if (curr != null && task != null) {
            if (curr.val == 0) {
                pending.remove(id);
                collector.emitDirect(task, ACKER_ACK_STREAM_ID, new Values(id));
            } else if (curr.failed) {
                pending.remove(id);
                collector.emitDirect(task, ACKER_FAIL_STREAM_ID, new Values(id));
            } else if(ACKER_RESET_TIMEOUT_STREAM_ID.equals(streamId)) {
                collector.emitDirect(task, ACKER_RESET_TIMEOUT_STREAM_ID, new Values(id));
            }
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        LOG.info("Acker: cleanup successfully");
    }
}
