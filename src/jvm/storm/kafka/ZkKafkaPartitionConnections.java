package storm.kafka;

import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.RuntimeException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkKafkaPartitionConnections extends KafkaPartitionConnections {
    private static int ZK_TIMEOUT = 10000;

    Map<String, Integer> _pathToHostIndex = new HashMap<String, Integer>();
    ZooKeeper _zkClient;
    String _brokerZkPath;

    // TODO: Only handles node data changes.
    // Should we handle the case where new producers enter or
    // old ones leave?
    Watcher eventWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeDataChanged) {
                String path = event.getPath();
                HostPort hp = getBrokerHost(path);
                int hostIndex = _pathToHostIndex.get(path);
                _kafka.put(hostIndex, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes));
            } else {
                throw new RuntimeException("Kafka producer node changed but don't know how to deal with event: " + event.getType());
            }
        }
    };

    public ZkKafkaPartitionConnections(KafkaConfig conf) {
        super(conf);
        _brokerZkPath = conf.brokerZkPath;
        try {
            _zkClient = new ZooKeeper(conf.brokerZkHost, 2000, eventWatcher);
        } catch(IOException e) {
            // Screw you checked exceptions!
            throw new RuntimeException("Error while creating zkClient: " + e);
        }
        setupHosts();
    }

    @Override
    public int getNumberOfHosts() {
        return _kafka.size();
    }

    @Override
    public SimpleConsumer getConsumer(int partition) {
        int hostIndex = partition / _config.partitionsPerHost;
        return _kafka.get(hostIndex);
    }

    @Override
    public void close() {
        super.close();
        try {
           _zkClient.close();
        } catch(InterruptedException e) {
           throw new RuntimeException("Error while closing client");
        }
    }

    private HostPort getBrokerHost(String zkPath) {
        try {
            String zkData = new String(_zkClient.getData(_brokerZkPath + "/" + zkPath, true, new Stat()));
            String[] hostString = zkData.split(":");
            String host = hostString[hostString.length - 2];
            int port = Integer.parseInt(hostString[hostString.length - 1]);
            return new HostPort(host, port);
        } catch(KeeperException e) {
            throw new RuntimeException("Could not get broker data: " + e);
        } catch(InterruptedException e) {
            throw new RuntimeException("Error while creating zkClient: " + e);
        }
    }

    private void setupHosts() {
        try {
            List<String> brokers = _zkClient.getChildren(_brokerZkPath, false);
            for(int i = 0; i < brokers.size(); i++) {
                String path = brokers.get(i);
                HostPort hp = getBrokerHost(path);
                _kafka.put(i, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes));
            }
        } catch(KeeperException e) {
            throw new RuntimeException("Could not get brokers: " + e);
        } catch(InterruptedException e) {
            throw new RuntimeException("Error while creating zkClient: " + e);
        }
    }
}
