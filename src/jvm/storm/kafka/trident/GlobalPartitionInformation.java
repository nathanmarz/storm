package storm.kafka.trident;

import storm.kafka.Broker;
import storm.kafka.Broker;
import storm.kafka.Partition;

import java.io.Serializable;
import java.util.*;

/**
 * Date: 14/05/2013
 * Time: 19:18
 */
public class GlobalPartitionInformation implements Iterable<Partition>, Serializable {

    private Map<Integer, Broker> partitionMap;

    public GlobalPartitionInformation() {
        partitionMap = new TreeMap<Integer, Broker>();
    }

    public void addPartition(int partitionId, Broker broker) {
        partitionMap.put(partitionId, broker);
    }

    @Override
    public String toString() {
        return "GlobalPartitionInformation{" +
                "partitionMap=" + partitionMap +
                '}';
    }

    public Broker getBrokerFor(Integer partitionId) {
        return partitionMap.get(partitionId);
    }

    public List<Partition> getOrderedPartitions() {
        List<Partition> partitions = new LinkedList<Partition>();
        for (Map.Entry<Integer, Broker> partition : partitionMap.entrySet()) {
            partitions.add(new Partition(partition.getValue(), partition.getKey()));
        }
        return partitions;
    }

    @Override
    public Iterator<Partition> iterator() {
        final Iterator<Map.Entry<Integer, Broker>> iterator = partitionMap.entrySet().iterator();

        return new Iterator<Partition>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Partition next() {
                Map.Entry<Integer, Broker> next = iterator.next();
                return new Partition(next.getValue(), next.getKey());
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((partitionMap == null) ? 0 : partitionMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GlobalPartitionInformation other = (GlobalPartitionInformation) obj;
        if (partitionMap == null) {
            if (other.partitionMap != null)
                return false;
        } else if (!partitionMap.equals(other.partitionMap))
            return false;
        return true;
    }
}
