package storm.kafka;


public class GlobalPartitionId {
    HostPort host;
    int partition;

    public GlobalPartitionId(HostPort host, int partition) {
        this.host = host;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        GlobalPartitionId other = (GlobalPartitionId) o;
        return host.equals(other.host) && partition == other.partition;
    }

    @Override
    public int hashCode() {
        return 13 * host.hashCode() + partition;
    }
    
    
}
