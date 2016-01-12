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
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MemoryTransactionalSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a transactional topology. It keeps a count of the number of tuples seen so far in a
 * database. The source of data and the databases are mocked out as in memory maps for demonstration purposes.
 *
 * @see <a href="http://storm.apache.org/documentation/Transactional-topologies.html">Transactional topologies</a>
 */
public class TransactionalGlobalCount {
  public static final int PARTITION_TAKE_PER_BATCH = 3;
  public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
    put(0, new ArrayList<List<Object>>() {{
      add(new Values("cat"));
      add(new Values("dog"));
      add(new Values("chicken"));
      add(new Values("cat"));
      add(new Values("dog"));
      add(new Values("apple"));
    }});
    put(1, new ArrayList<List<Object>>() {{
      add(new Values("cat"));
      add(new Values("dog"));
      add(new Values("apple"));
      add(new Values("banana"));
    }});
    put(2, new ArrayList<List<Object>>() {{
      add(new Values("cat"));
      add(new Values("cat"));
      add(new Values("cat"));
      add(new Values("cat"));
      add(new Values("cat"));
      add(new Values("dog"));
      add(new Values("dog"));
      add(new Values("dog"));
      add(new Values("dog"));
    }});
  }};

  public static class Value {
    int count = 0;
    BigInteger txid;
  }

  public static Map<String, Value> DATABASE = new HashMap<String, Value>();
  public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

  public static class BatchCount extends BaseBatchBolt {
    Object _id;
    BatchOutputCollector _collector;

    int _count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
      _collector = collector;
      _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
      _count++;
    }

    @Override
    public void finishBatch() {
      _collector.emit(new Values(_id, _count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "count"));
    }
  }

  public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
    TransactionAttempt _attempt;
    BatchOutputCollector _collector;

    int _sum = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
      _collector = collector;
      _attempt = attempt;
    }

    @Override
    public void execute(Tuple tuple) {
      _sum += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {
      Value val = DATABASE.get(GLOBAL_COUNT_KEY);
      Value newval;
      if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
        newval = new Value();
        newval.txid = _attempt.getTransactionId();
        if (val == null) {
          newval.count = _sum;
        }
        else {
          newval.count = _sum + val.count;
        }
        DATABASE.put(GLOBAL_COUNT_KEY, newval);
      }
      else {
        newval = val;
      }
      _collector.emit(new Values(_attempt, newval.count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "sum"));
    }
  }

  public static void main(String[] args) throws Exception {
    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
    builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");
    builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");

    LocalCluster cluster = new LocalCluster();

    Config config = new Config();
    config.setDebug(true);
    config.setMaxSpoutPending(3);

    cluster.submitTopology("global-count-topology", config, builder.buildTopology());

    Thread.sleep(3000);
    cluster.shutdown();
  }
}
