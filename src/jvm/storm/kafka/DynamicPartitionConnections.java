package storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.javaapi.consumer.SimpleConsumer;


public class DynamicPartitionConnections {
    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();
        
        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }
    
    Map<HostPort, ConnectionInfo> _connections = new HashMap();
    KafkaConfig _config;
    
    public DynamicPartitionConnections(KafkaConfig config) {
        _config = config;
    }
    
    public SimpleConsumer register(GlobalPartitionId id) {
        return register(id.host, id.partition);
    }
    
    public SimpleConsumer register(HostPort host, int partition) {
        if(!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes)));
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }
    
    public void unregister(HostPort port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if(info.partitions.isEmpty()) {
            info.consumer.close();
        }
        _connections.remove(port);
    }
    
    public void unregister(GlobalPartitionId id) {
        return unregister(id.host, id.partition);
    }
    
    public void clear() {
        for(ConnectionInfo info: _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
