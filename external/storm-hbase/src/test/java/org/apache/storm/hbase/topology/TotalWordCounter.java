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
package org.apache.storm.hbase.topology;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import static org.apache.storm.utils.Utils.tuple;

public class TotalWordCounter implements IBasicBolt {

    private BigInteger total = BigInteger.ZERO;
    private static final Logger LOG = LoggerFactory.getLogger(TotalWordCounter.class);
    private static final Random RANDOM = new Random();
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    /*
     * Just output the word value with a count of 1.
     * The HBaseBolt will handle incrementing the counter.
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        total = total.add(new BigInteger(input.getValues().get(1).toString()));
        collector.emit(tuple(total.toString()));
        //prints the total with low probability.
        if(RANDOM.nextInt(1000) > 995) {
            LOG.info("Running total = " + total);
        }
    }

    public void cleanup() {
        LOG.info("Final total = " + total);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("total"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
