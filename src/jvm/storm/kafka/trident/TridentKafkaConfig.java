package storm.kafka.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {


    public final IBatchCoordinator coordinator = new DefaultCoordinator();

    public TridentKafkaConfig(BrokerHosts hosts, String topic) {
        super(hosts, topic);
    }

	public TridentKafkaConfig(BrokerHosts hosts, String topic, String clientId) {
		super(hosts, topic, clientId);
	}

}
