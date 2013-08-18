package storm.kafka;

import storm.trident.spout.ISpoutPartition;

import java.util.Objects;


public class Partition implements ISpoutPartition {

	public final HostPort host;
    public final int partition;

    public Partition(HostPort host, int partition) {
        this.host = host;
        this.partition = partition;
    }

	@Override
	public int hashCode() {
		return Objects.hash(host, partition);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final Partition other = (Partition) obj;
		return Objects.equals(this.host, other.host) && Objects.equals(this.partition, other.partition);
	}

	@Override
	public String toString() {
		return "Partition{" +
				"host=" + host +
				", partition=" + partition +
				'}';
	}

    @Override
    public String getId() {
        return toString();
    }

}
