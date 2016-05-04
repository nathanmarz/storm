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
package org.apache.storm.drpc;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.Random;
import org.apache.storm.utils.Utils;


public class PrepareRequest extends BaseBasicBolt {
    public static final String ARGS_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String RETURN_STREAM = "ret";
    public static final String ID_STREAM = "id";

    Random rand;

    @Override
    public void prepare(Map map, TopologyContext context) {
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String args = tuple.getString(0);
        String returnInfo = tuple.getString(1);
        long requestId = rand.nextLong();
        collector.emit(ARGS_STREAM, new Values(requestId, args));
        collector.emit(RETURN_STREAM, new Values(requestId, returnInfo));
        collector.emit(ID_STREAM, new Values(requestId));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ARGS_STREAM, new Fields("request", "args"));
        declarer.declareStream(RETURN_STREAM, new Fields("request", "return"));
        declarer.declareStream(ID_STREAM, new Fields("request"));
    }
}
