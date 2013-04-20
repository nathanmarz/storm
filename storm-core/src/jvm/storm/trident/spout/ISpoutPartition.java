package storm.trident.spout;

public interface ISpoutPartition {
    /**
     * This is used as a Zookeeper node path for storing metadata.
     */
    String getId();
}
