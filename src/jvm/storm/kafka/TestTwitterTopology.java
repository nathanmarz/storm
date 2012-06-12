package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.StormSubmitter;
import java.util.ArrayList;
import java.util.List;

public class TestTwitterTopology {
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
        List<String> hosts = new ArrayList<String>();
        hosts.add("smf1-atc-13-sr1.prod.twitter.com");
        hosts.add("smf1-atx-26-sr1.prod.twitter.com");
        hosts.add("smf1-atw-05-sr4.prod.twitter.com");
        hosts.add("smf1-aty-37-sr4.prod.twitter.com");
        KafkaConfig kafkaConf = KafkaConfig.fromHostStrings(hosts, 8, "firehose");
        kafkaConf.scheme = new StringScheme();
        kafkaConf.forceStartOffsetTime(-1);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("id", "spout",
                new OpaqueTransactionalKafkaSpout(kafkaConf), 1);
        builder.setBolt("printer", new PrinterBolt())
                .shuffleGrouping("spout");
        Config config = new Config();
        
        StormSubmitter.submitTopology("kafka-test", config, builder.buildTopology());
        
    }
}
