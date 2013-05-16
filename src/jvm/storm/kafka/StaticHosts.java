package storm.kafka;

import storm.kafka.trident.GlobalPartitionInformation;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 11/05/2013
 * Time: 14:43
 */
public class StaticHosts implements BrokerHosts {


	private GlobalPartitionInformation partitionInformation;

	public StaticHosts(GlobalPartitionInformation partitionInformation) {
		this.partitionInformation = partitionInformation;
	}

	public GlobalPartitionInformation getPartitionInformation() {
		return partitionInformation;
	}
}
