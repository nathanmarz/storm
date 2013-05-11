package storm.kafka.trident;

import storm.kafka.HostPort;
import storm.kafka.StaticHosts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StaticBrokerReader implements IBrokerReader {

    Map<String, List> brokers = new HashMap();
    
    public StaticBrokerReader(StaticHosts hosts) {
        for(HostPort hp: hosts.hosts) {
            List info = new ArrayList();
            info.add((long) hp.port);
            info.add((long) hosts.partitionsPerHost);
            brokers.put(hp.host, info);
        }
    }
    
    @Override
    public Map<String, List> getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
