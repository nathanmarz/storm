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
package org.apache.storm.elasticsearch.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EsIndexBolt extends AbstractEsBolt {
    private static final Logger LOG = LoggerFactory.getLogger(EsIndexBolt.class);

    public EsIndexBolt(EsConfig esConfig) {
        super(esConfig);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String index = tuple.getStringByField("index");
            String type = tuple.getStringByField("type");
            String source = tuple.getStringByField("source");
            client.prepareIndex(index, type).setSource(source).execute().actionGet();
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
