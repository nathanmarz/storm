/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.*;

import static storm.kafka.KafkaUtils.taskId;

public class ZkCoordinator implements PartitionCoordinator {
    public static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    Map<Partition, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    ZkState _state;
    Map _stormConf;

    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        this(connections, stormConf, spoutConfig, state, taskIndex, totalTasks, topologyInstanceId, buildReader(stormConf, spoutConfig));
    }

    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId, DynamicBrokersReader reader) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _stormConf = stormConf;
        _state = state;
        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = reader;
    }

    private static DynamicBrokersReader buildReader(Map stormConf, SpoutConfig spoutConfig) {
        ZkHosts hosts = (ZkHosts) spoutConfig.hosts;
        return new DynamicBrokersReader(stormConf, hosts.brokerZkStr, hosts.brokerZkPath, spoutConfig.topic);
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    @Override
    public void refresh() {
        try {
            LOG.info(taskId(_taskIndex, _totalTasks) + "Refreshing partition manager connections");
            GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();
            List<Partition> mine = KafkaUtils.calculatePartitionsForTask(brokerInfo, _totalTasks, _taskIndex);

            Set<Partition> curr = _managers.keySet();
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            newPartitions.removeAll(curr);

            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            deletedPartitions.removeAll(mine);

            LOG.info(taskId(_taskIndex, _totalTasks) + "Deleted partition managers: " + deletedPartitions.toString());

            for (Partition id : deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            LOG.info(taskId(_taskIndex, _totalTasks) + "New partition managers: " + newPartitions.toString());

            for (Partition id : newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
                _managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info(taskId(_taskIndex, _totalTasks) + "Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
}
