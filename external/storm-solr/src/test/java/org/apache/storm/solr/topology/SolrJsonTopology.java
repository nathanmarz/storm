package org.apache.storm.solr.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.solr.bolt.SolrUpdateBolt;
import org.apache.storm.solr.mapper.SolrJsonMapper;
import org.apache.storm.solr.mapper.SolrMapper;
import org.apache.storm.solr.spout.SolrJsonSpout;

import java.io.IOException;

/**
 * Created by hlouro on 7/31/15.
 */
public class SolrJsonTopology extends SolrTopology {
    public static void main(String[] args) throws Exception {
        SolrJsonTopology solrJsonTopology = new SolrJsonTopology();
        solrJsonTopology.run(args);
    }

    protected SolrMapper getSolrMapper() throws IOException {
        final String jsonTupleField = "JSON";
        return new SolrJsonMapper(COLLECTION, jsonTupleField);
    }

    protected StormTopology getTopology() throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SolrJsonSpout", new SolrJsonSpout());
        builder.setBolt("SolrUpdateBolt", new SolrUpdateBolt(getSolrConfig(), getSolrMapper(), getSolrCommitStgy()))
                .shuffleGrouping("SolrJsonSpout");
        return builder.createTopology();
    }
}
