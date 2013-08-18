package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.IBrokerReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DynamicPartitionConnections {

	public static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);

    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();
        
        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }
    
    Map<HostPort, ConnectionInfo> _connections = new HashMap();
    KafkaConfig _config;
	IBrokerReader _reader;
    
    public DynamicPartitionConnections(KafkaConfig config, IBrokerReader brokerReader) {
        _config = config;
		_reader = brokerReader;
    }
    
    public SimpleConsumer register(Partition partition) {
		HostPort hostPort = _reader.getCurrentBrokers().getHostFor(partition.partition);
		return register(hostPort, partition.partition);
    }
    
    public SimpleConsumer register(HostPort host, int partition) {
        if(!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId)));
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }

    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if(info != null) return info.consumer;
        return null;
    }
    
    public void unregister(HostPort port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if(info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(port);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }
    
    public void clear() {
        for(ConnectionInfo info: _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
