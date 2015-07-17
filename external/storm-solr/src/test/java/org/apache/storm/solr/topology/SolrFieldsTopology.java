package org.apache.storm.solr.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.solr.bolt.SolrUpdateBolt;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.mapper.SolrFieldsMapper;
import org.apache.storm.solr.mapper.SolrMapper;
import org.apache.storm.solr.schema.builder.RestJsonSchemaBuilder;
import org.apache.storm.solr.spout.SolrFieldsSpout;

import java.io.IOException;

/**
 * Created by hlouro on 7/31/15.
 */
public class SolrFieldsTopology extends SolrTopology {
        public static void main(String[] args) throws Exception {
            SolrFieldsTopology solrJsonTopology = new SolrFieldsTopology();
            solrJsonTopology.run(args);
        }

    protected SolrMapper getSolrMapper() throws IOException {
        return new SolrFieldsMapper.Builder(
                new RestJsonSchemaBuilder("localhost", "8983", COLLECTION)).setCollection(COLLECTION).build();
    }

    protected SolrCommitStrategy getSolrCommitStgy() {
        return new CountBasedCommit(2);         // To Commit to Solr and Ack according to the commit strategy
    }

    protected StormTopology getTopology() throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SolrFieldsSpout", new SolrFieldsSpout());
        builder.setBolt("SolrUpdateBolt", new SolrUpdateBolt(getSolrConfig(), getSolrMapper(), getSolrCommitStgy()))
                .shuffleGrouping("SolrFieldsSpout");
        return builder.createTopology();
    }
}
