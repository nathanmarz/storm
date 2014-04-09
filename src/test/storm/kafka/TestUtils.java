package storm.kafka;

import storm.kafka.trident.GlobalPartitionInformation;

public class TestUtils {

    public static GlobalPartitionInformation buildPartitionInfo(int numPartitions) {
        return buildPartitionInfo(numPartitions, 9092);
    }


    public static GlobalPartitionInformation buildPartitionInfo(int numPartitions, int brokerPort) {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        for (int i = 0; i < numPartitions; i++) {
            globalPartitionInformation.addPartition(i, Broker.fromString("broker-" + i + " :" + brokerPort));
        }
        return globalPartitionInformation;
    }

}
