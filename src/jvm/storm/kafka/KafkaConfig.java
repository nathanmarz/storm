package storm.kafka;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

import java.io.Serializable;

public class KafkaConfig implements Serializable {

    public BrokerHosts hosts;
    public int fetchSizeBytes = 1024*1024;
    public int socketTimeoutMs = 10000;
    public int bufferSizeBytes = 1024*1024;
    public MultiScheme scheme = new RawMultiScheme();
    public String topic;
    public long startOffsetTime = -2;
    public boolean forceFromStart = false;

    public KafkaConfig(BrokerHosts hosts, String topic) {
        this.hosts = hosts;
        this.topic = topic;
    }

    public void forceStartOffsetTime(long millis) {
        startOffsetTime = millis;
        forceFromStart = true;
    }
}
