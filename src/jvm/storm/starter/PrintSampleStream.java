package storm.starter;

import storm.starter.spout.TwitterSampleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;


public class PrintSampleStream {        
    public static void main(String[] args) {
        String username = args[0];
        String pwd = args[1];
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(1, new TwitterSampleSpout(username, pwd));
                
        
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
