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

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;

/**
 * A simple example that demonstrates the usage of {@link org.apache.storm.topology.IStatefulWindowedBolt} to
 * save the state of the windowing operation to avoid re-computation in case of failures.
 * <p>
 * The framework internally manages the window boundaries and does not invoke
 * {@link org.apache.storm.topology.IWindowedBolt#execute(TupleWindow)} for the already evaluated windows in case of restarts
 * during failures. The {@link org.apache.storm.topology.IStatefulBolt#initState(State)}
 * is invoked with the previously saved state of the bolt after prepare, before the execute() method is invoked.
 * </p>
 */
public class StatefulWindowingTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowingTopology.class);

    private static class WindowSumBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Long>> {
        private KeyValueState<String, Long> state;
        private long sum;

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void initState(KeyValueState<String, Long> state) {
            this.state = state;
            sum = state.get("sum", 0L);
            LOG.debug("initState with state [" + state + "] current sum [" + sum + "]");
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            for (Tuple tuple : inputWindow.get()) {
                sum += tuple.getIntegerByField("value");
            }
            state.put("sum", sum);
            collector.emit(new Values(sum));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sum"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout());
        builder.setBolt("sumbolt", new WindowSumBolt().withWindow(new Count(5), new Count(3))
                .withMessageIdField("msgid"), 1).shuffleGrouping("spout");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("sumbolt");
        Config conf = new Config();
        conf.setDebug(false);
        //conf.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("test", conf, topology);
            Utils.sleep(40000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
