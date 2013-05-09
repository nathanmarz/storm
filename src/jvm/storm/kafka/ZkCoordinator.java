package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaConfig.ZkHosts;

public class ZkCoordinator implements PartitionCoordinator {
    public static Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);
    
    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    Map<GlobalPartitionId, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    ZkState _state;
    Map _stormConf;
    IMetricsContext _metricsContext;
    
    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _stormConf = stormConf;
	_state = state;

        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = new DynamicBrokersReader(stormConf, brokerConf.brokerZkStr, brokerConf.brokerZkPath, spoutConfig.topic);
        
    }
    
    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if(_lastRefreshTime==null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }
    
    void refresh() {
        try {
            LOG.info("Refreshing partition manager connections");
            Map<String, List> brokerInfo = _reader.getBrokerInfo();
            
            Set<GlobalPartitionId> mine = new HashSet();
            for(String host: brokerInfo.keySet()) {
                List info = brokerInfo.get(host);
                long port = (Long) info.get(0);
                HostPort hp = new HostPort(host, (int) port);
                long numPartitions = (Long) info.get(1);
                for(int i=0; i<numPartitions; i++) {
                    GlobalPartitionId id = new GlobalPartitionId(hp, i);
                    if(myOwnership(id)) {
                        mine.add(id);
                    }
                }
            }
            
            Set<GlobalPartitionId> curr = _managers.keySet();
            Set<GlobalPartitionId> newPartitions = new HashSet<GlobalPartitionId>(mine);
            newPartitions.removeAll(curr);
            
            Set<GlobalPartitionId> deletedPartitions = new HashSet<GlobalPartitionId>(curr);
            deletedPartitions.removeAll(mine);
            
            LOG.info("Deleted partition managers: " + deletedPartitions.toString());
            
            for(GlobalPartitionId id: deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            LOG.info("New partition managers: " + newPartitions.toString());
            
            for(GlobalPartitionId id: newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
                _managers.put(id, man);
            }
            
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info("Finished refreshing");
    }

    @Override
    public PartitionManager getManager(GlobalPartitionId id) {
        return _managers.get(id);        
    }
    
    private boolean myOwnership(GlobalPartitionId id) {
        int val = Math.abs(id.host.hashCode() + 23 * id.partition);        
        return val % _totalTasks == _taskIndex;
    } 
}
