package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.kafka.KafkaConfig.StaticHosts;


public class StaticCoordinator implements PartitionCoordinator {
    Map<GlobalPartitionId, PartitionManager> _managers = new HashMap<GlobalPartitionId, PartitionManager>();
    List<PartitionManager> _allManagers = new ArrayList();

    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        List<GlobalPartitionId> allPartitionIds = new ArrayList();
        for(HostPort h: hosts.hosts) {
            for(int i=0; i<hosts.partitionsPerHost; i++) {
                allPartitionIds.add(new GlobalPartitionId(h, i));
            }
        }
        for(int i=taskIndex; i<allPartitionIds.size(); i+=totalTasks) {
            GlobalPartitionId myPartition = allPartitionIds.get(i);
            _managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition));
            
        }
        
        _allManagers = new ArrayList(_managers.values());
    }
    
    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        return _allManagers;
    }
    
    public PartitionManager getManager(GlobalPartitionId id) {
        return _managers.get(id);
    }
    
}
