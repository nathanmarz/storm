package storm.kafka.trident;

import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;

import java.io.Serializable;
import java.util.*;

/**
 * Date: 14/05/2013
 * Time: 19:18
 */
public class GlobalPartitionInformation implements Iterable<GlobalPartitionId>, Serializable {

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

	public List<GlobalPartitionId> getOrderedPartitions(){
		List<GlobalPartitionId> partitionIds = new LinkedList<GlobalPartitionId>();
		for (Map.Entry<Integer, HostPort> partition : partitionMap.entrySet()) {
			partitionIds.add(new GlobalPartitionId(partition.getValue(), partition.getKey()));
		}
		return partitionIds;
	}

	@Override
	public Iterator<GlobalPartitionId> iterator() {
		final Iterator<Map.Entry<Integer, HostPort>> iterator = partitionMap.entrySet().iterator();

		return new Iterator<GlobalPartitionId>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public GlobalPartitionId next() {
				Map.Entry<Integer, HostPort> next = iterator.next();
				return new GlobalPartitionId(next.getValue(), next.getKey());
			}

			@Override
			public void remove() {
				iterator.remove();
			}
		};
	}
}
