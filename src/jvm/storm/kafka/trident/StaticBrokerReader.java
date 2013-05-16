package storm.kafka.trident;

import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.StaticHosts;

import java.util.List;


public class StaticBrokerReader implements IBrokerReader {

	GlobalPartitionInformation brokers = new GlobalPartitionInformation();
    
    public StaticBrokerReader(GlobalPartitionInformation partitionInformation) {
        this.brokers = partitionInformation;
    }
    
    @Override
    public GlobalPartitionInformation getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
