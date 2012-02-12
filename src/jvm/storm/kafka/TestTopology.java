package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import java.util.ArrayList;
import java.util.List;

public class TestTopology {
    public static void main(String [] args) throws Exception {
        List<String> hosts = new ArrayList<String>();
        hosts.add("localhost");
        KafkaConfig kafkaConf = new KafkaConfig(hosts, 3, "test");
        kafkaConf.scheme = new StringScheme();
        LocalCluster cluster = new LocalCluster();
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("id", "spout",
                new TransactionalKafkaSpout(kafkaConf), 1);
        
        Config config = new Config();
        config.setDebug(true);
        
        cluster.submitTopology("kafka-test", config, builder.buildTopology());
        
        Thread.sleep(600000);
    }
}
