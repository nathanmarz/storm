package storm.kafka.trident;

import storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {
    public TridentKafkaConfig(BrokerHosts hosts, String topic) {
        super(hosts, topic);
    }
    
    public IBatchCoordinator coordinator = new DefaultCoordinator();
}
