package org.apache.storm.solr.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.solr.config.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by hlouro on 7/17/15.
 */
public abstract class AbstractSolrBolt extends BaseRichBolt {
    protected OutputCollector collector;
    protected SolrConfig solrConfig;
    protected SolrClient solrClient;

    public AbstractSolrBolt(SolrConfig solrConfig) {
        this.solrConfig = solrConfig;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        solrClient = new CloudSolrClient(solrConfig.getZkHostString());
    }
}
