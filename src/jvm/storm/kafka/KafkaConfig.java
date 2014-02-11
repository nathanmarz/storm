package storm.kafka;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

import java.io.Serializable;

public class KafkaConfig implements Serializable {

    public final BrokerHosts hosts;
    public final String topic;
    public final String clientId;

    public int fetchSizeBytes = 1024 * 1024;
    public int socketTimeoutMs = 10000;
    public int bufferSizeBytes = 1024 * 1024;
    public MultiScheme scheme = new RawMultiScheme();
    public boolean forceFromStart = false;
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;

    public KafkaConfig(BrokerHosts hosts, String topic) {
        this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
    }

    public KafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        this.hosts = hosts;
        this.topic = topic;
        this.clientId = clientId;
    }

}
