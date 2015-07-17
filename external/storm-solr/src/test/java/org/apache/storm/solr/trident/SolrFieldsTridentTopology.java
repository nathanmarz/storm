package org.apache.storm.solr.trident;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.apache.storm.solr.spout.SolrFieldsSpout;
import org.apache.storm.solr.topology.SolrFieldsTopology;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.IOException;

/**
 * Created by hlouro on 7/31/15.
 */
public class SolrFieldsTridentTopology extends SolrFieldsTopology {
    public static void main(String[] args) throws Exception {
        SolrFieldsTridentTopology solrJsonTopology = new SolrFieldsTridentTopology();
        solrJsonTopology.run(args);
    }

    protected StormTopology getTopology() throws IOException {
        final TridentTopology topology = new TridentTopology();
        final SolrFieldsSpout spout = new SolrFieldsSpout();
        final Stream stream = topology.newStream("SolrFieldsSpout", spout);
        final StateFactory solrStateFactory = new SolrStateFactory(getSolrConfig(), getSolrMapper());
        stream.partitionPersist(solrStateFactory, spout.getOutputFields(),  new SolrUpdater(), new Fields());
        return topology.build();
    }
}
