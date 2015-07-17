package org.apache.storm.solr.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;

import java.io.IOException;

/**
 * Created by hlouro on 7/24/15.
 */
public abstract class SolrTopology {
    protected static String COLLECTION = "gettingstarted";
    protected static SolrClient solrClient = getSolrClient();

    public void run(String[] args) throws Exception {
        final StormTopology topology = getTopology();
        final Config config = getConfig();

        if (args.length == 0) {
            submitTopologyLocalCluster(topology, config);
        } else {
            submitTopologyRemoteCluster(args[1], topology, config);
        }
    }

    protected abstract StormTopology getTopology() throws IOException;

    protected void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    protected void submitTopologyLocalCluster(StormTopology topology, Config config) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        Thread.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
        System.exit(0);
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected SolrCommitStrategy getSolrCommitStgy() {
        return null;                          // To Commit to Solr and Ack every tuple
    }

    protected static SolrConfig getSolrConfig() {
        String zkHostString = "127.0.0.1:9983";  // zkHostString for Solr gettingstarted example
        return new SolrConfig(zkHostString);
    }

    protected static SolrClient getSolrClient() {
        String zkHostString = "127.0.0.1:9983";  // zkHostString for Solr gettingstarted example
        return new CloudSolrClient(zkHostString);
    }

}
