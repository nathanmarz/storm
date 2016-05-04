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

package org.apache.storm.solr.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrUpdateBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SolrUpdateBolt.class);

    /**
     * Half of the default Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
     */
    private static final int DEFAULT_TICK_TUPLE_INTERVAL_SECS = 15;

    private final SolrConfig solrConfig;
    private final SolrMapper solrMapper;
    private final SolrCommitStrategy commitStgy;    // if null, acks every tuple

    private SolrClient solrClient;
    private OutputCollector collector;
    private List<Tuple> toCommitTuples;
    private int tickTupleInterval = DEFAULT_TICK_TUPLE_INTERVAL_SECS;

    public SolrUpdateBolt(SolrConfig solrConfig, SolrMapper solrMapper) {
        this(solrConfig, solrMapper, null);
    }

    public SolrUpdateBolt(SolrConfig solrConfig, SolrMapper solrMapper, SolrCommitStrategy commitStgy) {
        this.solrConfig = solrConfig;
        this.solrMapper = solrMapper;
        this.commitStgy = commitStgy;
        LOG.debug("Created {} with the following configuration: " +
                    "[SolrConfig = {}], [SolrMapper = {}], [CommitStgy = {}]",
                    this.getClass().getSimpleName(), solrConfig, solrMapper, commitStgy);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.solrClient = new CloudSolrClient(solrConfig.getZkHostString());
        this.toCommitTuples = new ArrayList<>(capacity());
    }

    private int capacity() {
        final int defArrListCpcty = 10;
        return (commitStgy instanceof CountBasedCommit) ?
                ((CountBasedCommit)commitStgy).getThreshold() :
                defArrListCpcty;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (!TupleUtils.isTick(tuple)) {    // Don't add tick tuples to the SolrRequest
                SolrRequest request = solrMapper.toSolrRequest(tuple);
                solrClient.request(request, solrMapper.getCollection());
            }
            ack(tuple);
        } catch (Exception e) {
            fail(tuple, e);
        }
    }

    private void ack(Tuple tuple) throws SolrServerException, IOException {
        if (commitStgy == null) {
            collector.ack(tuple);
        } else {
            final boolean isTickTuple = TupleUtils.isTick(tuple);
            if (!isTickTuple) {    // Don't ack tick tuples
                toCommitTuples.add(tuple);
                commitStgy.update();
            }
            if (isTickTuple || commitStgy.commit()) {
                solrClient.commit(solrMapper.getCollection());
                ackCommittedTuples();
            }
        }
    }

    private void ackCommittedTuples() {
        List<Tuple> toAckTuples = getQueuedTuples();
        for (Tuple tuple : toAckTuples) {
            collector.ack(tuple);
        }
    }

    private void fail(Tuple tuple, Exception e) {
        collector.reportError(e);

        if (commitStgy == null) {
            collector.fail(tuple);
        } else {
            List<Tuple> failedTuples = getQueuedTuples();
            failQueuedTuples(failedTuples);
        }
    }

    private void failQueuedTuples(List<Tuple> failedTuples) {
        for (Tuple failedTuple : failedTuples) {
            collector.fail(failedTuple);
        }
    }

    private List<Tuple> getQueuedTuples() {
        List<Tuple> queuedTuples = toCommitTuples;
        toCommitTuples = new ArrayList<>(capacity());
        return queuedTuples;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (solrConfig.getTickTupleInterval() > 0) {
            this.tickTupleInterval = solrConfig.getTickTupleInterval();
        }
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), tickTupleInterval);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

}
