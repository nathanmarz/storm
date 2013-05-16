package storm.kafka.trident;

public interface IBrokerReader {

    GlobalPartitionInformation getCurrentBrokers();
    void close();
}
