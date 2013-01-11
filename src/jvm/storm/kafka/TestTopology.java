package storm.kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

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
//        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
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
