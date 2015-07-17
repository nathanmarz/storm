package org.apache.storm.solr.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by hlouro on 7/19/15.
 */
public class SolrUpdateBolt extends AbstractSolrBolt {
    private final Logger logger = LoggerFactory.getLogger(SolrUpdateBolt.class);
    private final SolrMapper solrMapper;
    private final SolrCommitStrategy commitStgy;
    private List<Tuple> toCommitTuples;
    private final String ackFailLock = "LOCK";      //serializable lock


    public SolrUpdateBolt(SolrConfig solrConfig, SolrMapper solrMapper) {
        this(solrConfig, solrMapper, null);
    }

    public SolrUpdateBolt(SolrConfig solrConfig, SolrMapper solrMapper, SolrCommitStrategy commitStgy) {
        super(solrConfig);
        this.solrMapper = solrMapper;
        this.commitStgy = commitStgy;
        logger.info("Created {} with the following configuration: " +
                    "[SolrConfig = {}], [SolrMapper = {}], [CommitStgy = {}]",
                    this.getClass().getSimpleName(), solrConfig, solrMapper, commitStgy);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.toCommitTuples = new LinkedList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            SolrRequest request = solrMapper.toSolrRequest(tuple);
            solrClient.request(request, solrMapper.getCollection());
            ack(tuple);
        } catch (Exception e) {
            fail(tuple, e);
        }
    }

    private void ack(Tuple tuple) throws SolrServerException, IOException {
        if (commitStgy == null) {
            collector.ack(tuple);
        } else {
            synchronized(ackFailLock) {
                toCommitTuples.add(tuple);
                commitStgy.update();
            }
            if (commitStgy.commit()) {
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
            for (Tuple failedTuple : failedTuples) {
                collector.fail(failedTuple);
            }
        }
    }

    private List<Tuple> getQueuedTuples() {
        List<Tuple> queuedTuples;
        synchronized(ackFailLock) {
            queuedTuples = toCommitTuples;
            toCommitTuples = new LinkedList<>();
        }
        return queuedTuples;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
