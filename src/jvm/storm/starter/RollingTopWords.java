package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.MergeObjects;
import storm.starter.bolt.RankObjects;
import storm.starter.bolt.RollingCountObjects;


/**
 * This topology does a continuous computation of the top N words that the topology has seen 
 * in terms of cardinality. The top N computation is done in a completely scalable way, and 
 * a similar approach could be used to compute things like trending topics or trending images
 * on Twitter.
 */
public class RollingTopWords {
    
    public static void main(String[] args) throws Exception {
        
        final int TOP_N = 3;
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new TestWordSpout(), 5);
        
        builder.setBolt("count", new RollingCountObjects(60, 10), 4)
                 .fieldsGrouping("word", new Fields("word"));
        builder.setBolt("rank", new RankObjects(TOP_N), 4)
                 .fieldsGrouping("count", new Fields("obj"));
        builder.setBolt("merge", new MergeObjects(TOP_N))
                 .globalGrouping("rank");
        
        
        
        Config conf = new Config();
        conf.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("rolling-demo", conf, builder.createTopology());
        Thread.sleep(10000);
        
        cluster.shutdown();
        
    }    
}
