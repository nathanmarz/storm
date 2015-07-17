package org.apache.storm.solr.trident;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.apache.storm.solr.spout.SolrJsonSpout;
import org.apache.storm.solr.topology.SolrJsonTopology;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.IOException;

/**
 * Created by hlouro on 7/31/15.
 */
public class SolrJsonTridentTopology extends SolrJsonTopology {
    public static void main(String[] args) throws Exception {
        SolrJsonTridentTopology solrJsonTopology = new SolrJsonTridentTopology();
        solrJsonTopology.run(args);
    }

    protected StormTopology getTopology() throws IOException {
        final TridentTopology topology = new TridentTopology();
        final SolrJsonSpout spout = new SolrJsonSpout();
        final Stream stream = topology.newStream("SolrJsonSpout", spout);
        final StateFactory solrStateFactory = new SolrStateFactory(getSolrConfig(), getSolrMapper());
        stream.partitionPersist(solrStateFactory, spout.getOutputFields(),  new SolrUpdater(), new Fields());
        return topology.build();
    }
}
