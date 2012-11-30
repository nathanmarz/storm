package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeToMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;
import storm.kafka.KafkaConfig.StaticHosts;

public class TestTopology {
    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }
        
    }
    
    public static void main(String [] args) throws Exception {
//        List<String> hosts = new ArrayList<String>();
//        hosts.add("localhost");
//        KafkaConfig kafkaConf = new KafkaConfig(StaticHosts.fromHostString(hosts, 3), "test");
//        kafkaConf.scheme = new SchemeToMultiScheme(new StringScheme());
//        LocalCluster cluster = new LocalCluster();
//        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("id", "spout",
//                new OpaqueTransactionalKafkaSpout(kafkaConf), 1);
//        builder.setBolt("printer", new PrinterBolt())
//                .shuffleGrouping("spout");
//        Config config = new Config();
//        
//        cluster.submitTopology("kafka-test", config, builder.buildTopology());
//        
//        Thread.sleep(600000);
    }
}
