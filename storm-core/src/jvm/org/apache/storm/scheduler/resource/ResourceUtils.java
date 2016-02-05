/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ResourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    public static Map<String, Map<String, Double>> getBoltsResources(StormTopology topology, Map topologyConf) {
        Map<String, Map<String, Double>> boltResources = new HashMap<String, Map<String, Double>>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Map<String, Double> topology_resources = parseResources(bolt.getValue().get_common().get_json_conf());
                checkIntialization(topology_resources, bolt.getValue().toString(), topologyConf);
                boltResources.put(bolt.getKey(), topology_resources);
            }
        }
        return boltResources;
    }

    public static Map<String, Map<String, Double>> getSpoutsResources(StormTopology topology, Map topologyConf) {
        Map<String, Map<String, Double>> spoutResources = new HashMap<String, Map<String, Double>>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                Map<String, Double> topology_resources = parseResources(spout.getValue().get_common().get_json_conf());
                checkIntialization(topology_resources, spout.getValue().toString(), topologyConf);
                spoutResources.put(spout.getKey(), topology_resources);
            }
        }
        return spoutResources;
    }

    public static void checkIntialization(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        checkInitMem(topology_resources, Com, topologyConf);
        checkInitCPU(topology_resources, Com, topologyConf);
    }

    public static void checkInitMem(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                    org.apache.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null));
            debugMessage("ONHEAP", Com, topologyConf);
        }
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                    org.apache.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null));
            debugMessage("OFFHEAP", Com, topologyConf);
        }
    }

    public static void checkInitCPU(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                    org.apache.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null));
            debugMessage("CPU", Com, topologyConf);
        }
    }

    public static Map<String, Double> parseResources(String input) {
        Map<String, Double> topology_resources = new HashMap<String, Double>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = org.apache.storm.utils.Utils
                            .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = org.apache.storm.utils.Utils
                            .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCPU = org.apache.storm.utils.Utils.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCPU);
                }
                LOG.debug("Topology Resources {}", topology_resources);
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topology_resources;
    }

    private static void debugMessage(String memoryType, String Com, Map topologyConf) {
        if (memoryType.equals("ONHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : On Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        } else if (memoryType.equals("OFFHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : Off Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        } else {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : CPU Pcore Percent set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        }
    }

    /**
     * print scheduling for debug purposes
     * @param cluster
     * @param topologies
     */
    public static String printScheduling(Cluster cluster, Topologies topologies) {
        StringBuilder str = new StringBuilder();
        Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap = new HashMap<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>>();
        for (TopologyDetails topo : topologies.getTopologies()) {
            if (cluster.getAssignmentById(topo.getId()) != null) {
                for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
                    WorkerSlot slot = entry.getValue();
                    String nodeId = slot.getNodeId();
                    ExecutorDetails exec = entry.getKey();
                    if (!schedulingMap.containsKey(nodeId)) {
                        schedulingMap.put(nodeId, new HashMap<String, Map<WorkerSlot, Collection<ExecutorDetails>>>());
                    }
                    if (schedulingMap.get(nodeId).containsKey(topo.getId()) == false) {
                        schedulingMap.get(nodeId).put(topo.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());
                    }
                    if (schedulingMap.get(nodeId).get(topo.getId()).containsKey(slot) == false) {
                        schedulingMap.get(nodeId).get(topo.getId()).put(slot, new LinkedList<ExecutorDetails>());
                    }
                    schedulingMap.get(nodeId).get(topo.getId()).get(slot).add(exec);
                }
            }
        }

        for (Map.Entry<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> entry : schedulingMap.entrySet()) {
            if (cluster.getSupervisorById(entry.getKey()) != null) {
                str.append("/** Node: " + cluster.getSupervisorById(entry.getKey()).getHost() + "-" + entry.getKey() + " **/\n");
            } else {
                str.append("/** Node: Unknown may be dead -" + entry.getKey() + " **/\n");
            }
            for (Map.Entry<String, Map<WorkerSlot, Collection<ExecutorDetails>>> topo_sched : schedulingMap.get(entry.getKey()).entrySet()) {
                str.append("\t-->Topology: " + topo_sched.getKey() + "\n");
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> ws : topo_sched.getValue().entrySet()) {
                    str.append("\t\t->Slot [" + ws.getKey().getPort() + "] -> " + ws.getValue() + "\n");
                }
            }
        }
        return str.toString();
    }
}
