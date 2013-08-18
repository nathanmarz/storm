package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StaticCoordinator implements PartitionCoordinator {
    Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
    List<PartitionManager> _allManagers = new ArrayList();

    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        List<Partition> partitions = hosts.getPartitionInformation().getOrderedPartitions();
        for(int i=taskIndex; i<partitions.size(); i+=totalTasks) {
            Partition myPartition = partitions.get(i);
            _managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition));
            
        }
        
        _allManagers = new ArrayList(_managers.values());
    }
    
    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        return _allManagers;
    }
    
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
    
}
