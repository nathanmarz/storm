package storm.kafka.trident;

import java.util.List;
import java.util.Map;


public interface IBrokerReader {    
    /**
     * Map of host to [port, numPartitions]
     */
    Map<String, List> getCurrentBrokers();
    void close();
}
