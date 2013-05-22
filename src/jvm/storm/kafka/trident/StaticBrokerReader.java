package storm.kafka.trident;

public class StaticBrokerReader implements IBrokerReader {

	private GlobalPartitionInformation brokers = new GlobalPartitionInformation();
    
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
