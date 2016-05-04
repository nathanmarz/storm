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
package org.apache.storm.mongodb.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt for writing to MongoDB.
 *
 * Note: Each MongoInsertBolt defined in a topology is tied to a specific collection.
 *
 */
public class MongoInsertBolt extends AbstractMongoBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MongoInsertBolt.class);

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private MongoMapper mapper;

    private boolean ordered = true;  //default is ordered.

    private int batchSize = 15000;

    private List<Tuple> tupleBatch;

    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;

    public MongoInsertBolt(String url, String collectionName, MongoMapper mapper) {
        super(url, collectionName);

        Validate.notNull(mapper, "MongoMapper can not be null");

        this.mapper = mapper;

        this.tupleBatch = new LinkedList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        boolean forceFlush = false;
        try{
            if (TupleUtils.isTick(tuple)) {
                LOG.debug("TICK received! current batch status [{}/{}]", tupleBatch.size(), batchSize);
                collector.ack(tuple);
                forceFlush = true;
            } else {
                tupleBatch.add(tuple);
                if (tupleBatch.size() >= batchSize) {
                    forceFlush = true;
                }
            }

            if(forceFlush && !tupleBatch.isEmpty()) {
                List<Document> docs = new LinkedList<>();
                for (Tuple t : tupleBatch) {
                    Document doc = mapper.toDocument(t);
                    docs.add(doc);
                }
                mongoClient.insert(docs, ordered);

                for(Tuple t : tupleBatch) {
                    collector.ack(t);
                }
                tupleBatch.clear();
            }
        } catch (Exception e) {
            this.collector.reportError(e);
            for (Tuple t : tupleBatch) {
                collector.fail(t);
            }
            tupleBatch.clear();
        }
    }

    public MongoInsertBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public MongoInsertBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public MongoInsertBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

}
