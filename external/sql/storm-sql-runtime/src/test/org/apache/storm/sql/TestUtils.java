/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * <p>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.storm.sql;

import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestUtils {
  public static class MockDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockDataSource() {
      for (int i = 0; i < 5; ++i) {
        RECORDS.add(new Values(i));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.fireChannelInactive();
    }
  }

  public static class MockSqlTridentDataSource implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockSpout();
    }

    @Override
    public Function getConsumer() {
      return new CollectDataFunction();
    }

    public static class CollectDataFunction extends BaseFunction {
      /**
       * Collect all values in a static variable as the instance will go through serialization and deserialization.
       */
      private transient static final List<List<Object> > VALUES = new ArrayList<>();
      public static List<List<Object>> getCollectedValues() {
        return VALUES;
      }

      @Override
      public void execute(TridentTuple tuple, TridentCollector collector) {
        VALUES.add(tuple.getValues());
      }
    }

    private static class MockSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("ID");

      public MockSpout() {
        for (int i = 0; i < 5; ++i) {
          RECORDS.add(new Values(i));
        }
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class CollectDataChannelHandler implements ChannelHandler {
    private final List<Values> values;

    public CollectDataChannelHandler(List<Values> values) {
      this.values = values;
    }

    @Override
    public void dataReceived(ChannelContext ctx, Values data) {
      values.add(data);
    }

    @Override
    public void channelInactive(ChannelContext ctx) {}

    @Override
    public void exceptionCaught(Throwable cause) {
      throw new RuntimeException(cause);
    }
  }

  public static long monotonicNow() {
    final long NANOSECONDS_PER_MILLISECOND = 1000000;
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }

  public static ILocalCluster newLocalCluster() {
    return new LocalCluster();
  }
}
