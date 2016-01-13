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
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
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
 * This class defines a more involved transactional topology then TransactionalGlobalCount. This topology processes a
 * stream of words and produces two outputs:
 * <p/>
 * 1. A count for each word (stored in a database) 2. The number of words for every bucket of 10 counts. So it stores in
 * the database how many words have appeared 0-9 times, how many have appeared 10-19 times, and so on.
 * <p/>
 * A batch of words can cause the bucket counts to decrement for some buckets and increment for others as words move
 * between buckets as their counts accumulate.
 */
public class TransactionalWords {
  public static class CountValue {
    Integer prev_count = null;
    int count = 0;
    BigInteger txid = null;
  }

  public static class BucketValue {
    int count = 0;
    BigInteger txid;
  }

  public static final int BUCKET_SIZE = 10;

  public static Map<String, CountValue> COUNT_DATABASE = new HashMap<String, CountValue>();
  public static Map<Integer, BucketValue> BUCKET_DATABASE = new HashMap<Integer, BucketValue>();


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

  public static class KeyedCountUpdater extends BaseTransactionalBolt implements ICommitter {
    Map<String, Integer> _counts = new HashMap<String, Integer>();
    BatchOutputCollector _collector;
    TransactionAttempt _id;

    int _count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
      _collector = collector;
      _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(1);
      Integer curr = _counts.get(key);
      if (curr == null)
        curr = 0;
      _counts.put(key, curr + 1);
    }

    @Override
    public void finishBatch() {
      for (String key : _counts.keySet()) {
        CountValue val = COUNT_DATABASE.get(key);
        CountValue newVal;
        if (val == null || !val.txid.equals(_id)) {
          newVal = new CountValue();
          newVal.txid = _id.getTransactionId();
          if (val != null) {
            newVal.prev_count = val.count;
            newVal.count = val.count;
          }
          newVal.count = newVal.count + _counts.get(key);
          COUNT_DATABASE.put(key, newVal);
        }
        else {
          newVal = val;
        }
        _collector.emit(new Values(_id, key, newVal.count, newVal.prev_count));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "key", "count", "prev-count"));
    }
  }

  public static class Bucketize extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      TransactionAttempt attempt = (TransactionAttempt) tuple.getValue(0);
      int curr = tuple.getInteger(2);
      Integer prev = tuple.getInteger(3);

      int currBucket = curr / BUCKET_SIZE;
      Integer prevBucket = null;
      if (prev != null) {
        prevBucket = prev / BUCKET_SIZE;
      }

      if (prevBucket == null) {
        collector.emit(new Values(attempt, currBucket, 1));
      }
      else if (currBucket != prevBucket) {
        collector.emit(new Values(attempt, currBucket, 1));
        collector.emit(new Values(attempt, prevBucket, -1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("attempt", "bucket", "delta"));
    }
  }

  public static class BucketCountUpdater extends BaseTransactionalBolt {
    Map<Integer, Integer> _accum = new HashMap<Integer, Integer>();
    BatchOutputCollector _collector;
    TransactionAttempt _attempt;

    int _count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
      _collector = collector;
      _attempt = attempt;
    }

    @Override
    public void execute(Tuple tuple) {
      Integer bucket = tuple.getInteger(1);
      Integer delta = tuple.getInteger(2);
      Integer curr = _accum.get(bucket);
      if (curr == null)
        curr = 0;
      _accum.put(bucket, curr + delta);
    }

    @Override
    public void finishBatch() {
      for (Integer bucket : _accum.keySet()) {
        BucketValue currVal = BUCKET_DATABASE.get(bucket);
        BucketValue newVal;
        if (currVal == null || !currVal.txid.equals(_attempt.getTransactionId())) {
          newVal = new BucketValue();
          newVal.txid = _attempt.getTransactionId();
          newVal.count = _accum.get(bucket);
          if (currVal != null)
            newVal.count += currVal.count;
          BUCKET_DATABASE.put(bucket, newVal);
        }
        else {
          newVal = currVal;
        }
        _collector.emit(new Values(_attempt, bucket, newVal.count));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "bucket", "count"));
    }
  }

  public static void main(String[] args) throws Exception {
    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("top-n-words", "spout", spout, 2);
    builder.setBolt("count", new KeyedCountUpdater(), 5).fieldsGrouping("spout", new Fields("word"));
    builder.setBolt("bucketize", new Bucketize()).noneGrouping("count");
    builder.setBolt("buckets", new BucketCountUpdater(), 5).fieldsGrouping("bucketize", new Fields("bucket"));


    LocalCluster cluster = new LocalCluster();

    Config config = new Config();
    config.setDebug(true);
    config.setMaxSpoutPending(3);

    cluster.submitTopology("top-n-topology", config, builder.buildTopology());

    Thread.sleep(3000);
    cluster.shutdown();
  }
}
