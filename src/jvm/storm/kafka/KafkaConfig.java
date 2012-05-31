package storm.kafka;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KafkaConfig implements Serializable {
    public List<HostPort> hosts;
    public int partitionsPerHost;
    public int fetchSizeBytes = 1024*1024;
    public int socketTimeoutMs = 10000;
    public int bufferSizeBytes = 1024*1024;
    public Scheme scheme = new RawScheme();
    public String topic;
    public long startOffsetTime = -2;
    public boolean forceFromStart = false;

    public KafkaConfig(List<HostPort> hosts, int partitionsPerHost, String topic) {
        this.hosts = hosts;
        this.partitionsPerHost = partitionsPerHost;
        this.topic = topic;
    }
    
    public static KafkaConfig fromHostStrings(List<String> hosts, int partitionsPerHost, String topic) {
        return new KafkaConfig(convertHosts(hosts), partitionsPerHost, topic);
    }
    
    public void forceStartOffsetTime(long millis) {
        startOffsetTime = millis;
        forceFromStart = true;
    }
    
    public static List<HostPort> convertHosts(List<String> hosts) {
        List<HostPort> ret = new ArrayList<HostPort>();
        for(String s: hosts) {
            HostPort hp;
            String[] spec = s.split(":");
            if(spec.length==1) {
                hp = new HostPort(spec[0]);
            } else if (spec.length==2) {
                hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
            } else {
                throw new IllegalArgumentException("Invalid host specification: " + s);
            }
        }
        return ret;
    }
}
