package storm.kafka;

import java.util.List;

public interface PartitionCoordinator {
    List<PartitionManager> getMyManagedPartitions();
    PartitionManager getManager(GlobalPartitionId id);
}
