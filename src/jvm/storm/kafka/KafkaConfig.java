package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

public class KafkaConfig implements Serializable {
    public static interface BrokerHosts extends Serializable {
        
    }
    
    public static class StaticHosts implements BrokerHosts {
        public static int getNumHosts(BrokerHosts hosts) {
            if(!(hosts instanceof StaticHosts)) {
                throw new RuntimeException("Must use static hosts");
            }
            return ((StaticHosts) hosts).hosts.size();
        }
        
        public List<HostPort> hosts;
        public int partitionsPerHost;
        
        public static StaticHosts fromHostString(List<String> hostStrings, int partitionsPerHost) {
            return new StaticHosts(convertHosts(hostStrings), partitionsPerHost);
        }
        
        public StaticHosts(List<HostPort> hosts, int partitionsPerHost) {
            this.hosts = hosts;
            this.partitionsPerHost = partitionsPerHost;
        }

    }

    public static class ZkHosts implements BrokerHosts {
        public String brokerZkStr = null;        
        public String brokerZkPath = null; // e.g., /kafka/brokers
        public int refreshFreqSecs = 60;
        
        public ZkHosts(String brokerZkStr, String brokerZkPath) {
            this.brokerZkStr = brokerZkStr;
            this.brokerZkPath = brokerZkPath;
        }
    }

    
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
            ret.add(hp);
        }
        return ret;
    }
}
