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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.bolt.PrinterBolt;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static backtype.storm.topology.base.BaseWindowedBolt.Count;

/**
 * A sample topology that demonstrates the usage of {@link backtype.storm.topology.IWindowedBolt}
 * to calculate sliding window sum.
 */
public class SlidingWindowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowTopology.class);

    /*
     * emits a random integer every 100 ms
     */

    private static class RandomIntegerSpout extends BaseRichSpout {
        SpoutOutputCollector collector;
        Random rand;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.rand = new Random();
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            collector.emit(new Values(rand.nextInt(1000)));
        }
    }

    /*
     * Computes sliding window sum
     */
    private static class SlidingWindowSumBolt extends BaseWindowedBolt {
        private int sum = 0;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            /*
             * The inputWindow gives a view of
             * (a) all the events in the window
             * (b) events that expired since last activation of the window
             * (c) events that newly arrived since last activation of the window
             */
            List<Tuple> tuplesInWindow = inputWindow.get();
            List<Tuple> newTuples = inputWindow.getNew();
            List<Tuple> expiredTuples = inputWindow.getExpired();

            LOG.debug("Events in current window: " + tuplesInWindow.size());
            /*
             * Instead of iterating over all the tuples in the window to compute
             * the sum, the values for the new events are added and old events are
             * subtracted. Similar optimizations might be possible in other
             * windowing computations.
             */
            for (Tuple tuple : newTuples) {
                sum += (int) tuple.getValue(0);
            }
            for (Tuple tuple : expiredTuples) {
                sum -= (int) tuple.getValue(0);
            }
            collector.emit(new Values(sum));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sum"));
        }
    }


    /*
     * Computes tumbling window average
     */
    private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            List<Tuple> tuplesInWindow = inputWindow.get();
            LOG.debug("Events in current window: " + tuplesInWindow.size());
            if (tuplesInWindow.size() > 0) {
                /*
                * Since this is a tumbling window calculation,
                * we use all the tuples in the window to compute the avg.
                */
                for (Tuple tuple : tuplesInWindow) {
                    sum += (int) tuple.getValue(0);
                }
                collector.emit(new Values(sum / tuplesInWindow.size()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RandomIntegerSpout(), 1);
        builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(new Count(30), new Count(10)), 1)
                .shuffleGrouping("integer");
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withTumblingWindow(new Count(3)), 1)
                .shuffleGrouping("slidingsum");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(40000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
