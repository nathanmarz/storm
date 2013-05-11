package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import scala.actors.threadpool.Arrays;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.functions.PrintFunction;
import storm.trident.TridentTopology;

import java.util.List;

public class StaticKafkaTestTopology {

	public static void main(String[] args) throws Exception {

		List<String> hosts = Arrays.asList(new String[]{"localhost"});
		BrokerHosts brokerHosts = StaticHosts.fromHostString(hosts, 1);
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "testTopology");

		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
		topology.newStream("kafka", kafkaSpout).each(new Fields("str"), new PrintFunction(), new Fields("lines"));
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka", config, topology.build());
	}
}
