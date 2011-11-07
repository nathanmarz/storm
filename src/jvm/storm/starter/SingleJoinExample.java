package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.SingleJoinBolt;

public class SingleJoinExample {
    public static void main(String[] args) {
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(1, genderSpout);
        builder.setSpout(2, ageSpout);
        builder.setBolt(3, new SingleJoinBolt(new Fields("gender", "age")))
                .fieldsGrouping(1, new Fields("id"))
                .fieldsGrouping(2, new Fields("id"));
        
        Config conf = new Config();
        conf.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-example", conf, builder.createTopology());
        
        for(int i=0; i<10; i++) {
            String gender;
            if(i % 2 == 0) {
                gender = "male";
            } else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }
        
        for(int i=9; i>=0; i--) {            
            ageSpout.feed(new Values(i, i+20));
        }
        
        Utils.sleep(2000);
        cluster.shutdown();
    }
}
