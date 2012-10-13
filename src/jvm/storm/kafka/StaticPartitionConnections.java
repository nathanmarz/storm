package storm.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;
import storm.kafka.KafkaConfig.StaticHosts;

public class StaticPartitionConnections {
    Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
    KafkaConfig _config;
    StaticHosts hosts;
    
    public StaticPartitionConnections(KafkaConfig conf) {
        _config = conf;
        if(!(conf.hosts instanceof KafkaConfig.StaticHosts)) {
            throw new RuntimeException("Must configure with static hosts");
        }
        this.hosts = (StaticHosts) conf.hosts;
    }

    public SimpleConsumer getConsumer(int partition) {
        int hostIndex = partition / hosts.partitionsPerHost;
        if(!_kafka.containsKey(hostIndex)) {
            HostPort hp = hosts.hosts.get(hostIndex);
            _kafka.put(hostIndex, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes));

        }
        return _kafka.get(hostIndex);
    }

    public int getHostPartition(int globalPartition) {
        return globalPartition % hosts.partitionsPerHost;
    }

    public int getNumberOfHosts() {
        return hosts.hosts.size();
    }

    public void close() {
        for(SimpleConsumer consumer: _kafka.values()) {
            consumer.close();
        }
    }
}
