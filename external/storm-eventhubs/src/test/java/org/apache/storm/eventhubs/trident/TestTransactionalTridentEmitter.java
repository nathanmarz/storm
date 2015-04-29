/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.trident;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.storm.eventhubs.spout.EventHubReceiverMock;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventHubReceiver;
import org.apache.storm.eventhubs.spout.IEventHubReceiverFactory;

public class TestTransactionalTridentEmitter {
  private TransactionalTridentEventHubEmitter emitter;
  private Partition partition;
  private TridentCollectorMock collectorMock;
  private final int batchSize = 32;

  @Before
  public void setUp() throws Exception {
    EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "password",
        "namespace", "entityname", 16, "zookeeper");
    conf.setTopologyName("TestTopo");
    IEventHubReceiverFactory recvFactory = new IEventHubReceiverFactory() {
      @Override
      public IEventHubReceiver create(EventHubSpoutConfig config,
          String partitionId) {
        return new EventHubReceiverMock(partitionId);
      }
    };
    partition = new Partition(conf, "0");
    emitter = new TransactionalTridentEventHubEmitter(conf, batchSize, null, recvFactory);
    collectorMock = new TridentCollectorMock();
  }

  @After
  public void tearDown() throws Exception {
    emitter.close();
    emitter = null;
  }

  @Test
  public void testEmitInSequence() {
    //test the happy path, emit batches in sequence
    Map meta = emitter.emitPartitionBatchNew(null, collectorMock, partition, null);
    String collected = collectorMock.getBuffer();
    assertTrue(collected.startsWith("message"+0));
    //System.out.println("collected: " + collected);
    collectorMock.clear();
    
    emitter.emitPartitionBatchNew(null, collectorMock, partition, meta);
    collected = collectorMock.getBuffer();
    //System.out.println("collected: " + collected);
    assertTrue(collected.startsWith("message"+batchSize));
  }

  @Test
  public void testReEmit() {
    //test we can re-emit the second batch
    Map meta = emitter.emitPartitionBatchNew(null, collectorMock, partition, null);
    collectorMock.clear();
    
    //emit second batch
    Map meta1 = emitter.emitPartitionBatchNew(null, collectorMock, partition, meta);
    String collected0 = collectorMock.getBuffer();
    collectorMock.clear();
    
    //re-emit second batch
    emitter.emitPartitionBatch(null, collectorMock, partition, meta1);
    String collected1 = collectorMock.getBuffer();
    assertTrue(collected0.equals(collected1));
  }
}
