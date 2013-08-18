package storm.kafka.trident;

import storm.kafka.Partition;
import storm.kafka.HostPort;

import java.io.Serializable;
import java.util.*;

/**
 * Date: 14/05/2013
 * Time: 19:18
 */
public class GlobalPartitionInformation implements Iterable<Partition>, Serializable {

	private Map<Integer, HostPort> partitionMap;

	public GlobalPartitionInformation() {
		partitionMap = new TreeMap<Integer, HostPort>();
	}

	public void addPartition(int partitionId, HostPort broker) {
		partitionMap.put(partitionId, broker);
	}

	@Override
	public String toString() {
		return "GlobalPartitionInformation{" +
				"partitionMap=" + partitionMap +
				'}';
	}

	public HostPort getHostFor(Integer partitionId) {
		return partitionMap.get(partitionId);
	}

	public List<Partition> getOrderedPartitions(){
		List<Partition> partitions = new LinkedList<Partition>();
		for (Map.Entry<Integer, HostPort> partition : partitionMap.entrySet()) {
			partitions.add(new Partition(partition.getValue(), partition.getKey()));
		}
		return partitions;
	}

	@Override
	public Iterator<Partition> iterator() {
		final Iterator<Map.Entry<Integer, HostPort>> iterator = partitionMap.entrySet().iterator();

		return new Iterator<Partition>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Partition next() {
				Map.Entry<Integer, HostPort> next = iterator.next();
				return new Partition(next.getValue(), next.getKey());
			}

			@Override
			public void remove() {
				iterator.remove();
			}
		};
	}
}
