package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

public class StaticPartitionConnections {
    Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
    KafkaConfig _config;
    StaticHosts hosts;
    
    public StaticPartitionConnections(KafkaConfig conf) {
        _config = conf;
        if(!(conf.hosts instanceof StaticHosts)) {
            throw new RuntimeException("Must configure with static hosts");
        }
        this.hosts = (StaticHosts) conf.hosts;
    }

    public SimpleConsumer getConsumer(int partition) {
		if(!_kafka.containsKey(partition)) {
            HostPort hp = hosts.getPartitionInformation().getHostFor(partition);
            _kafka.put(partition, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId));

        }
        return _kafka.get(partition);
    }

    public void close() {
        for(SimpleConsumer consumer: _kafka.values()) {
            consumer.close();
        }
    }
}
