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
package org.apache.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.resource.Component;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TopologyDetails {
    private String topologyId;
    private Map topologyConf;
    private StormTopology topology;
    private Map<ExecutorDetails, String> executorToComponent;
    private int numWorkers;
    //<ExecutorDetails - Task, Map<String - Type of resource, Map<String - type of that resource, Double - amount>>>
    private Map<ExecutorDetails, Map<String, Double>> resourceList;
    //Max heap size for a worker used by topology
    private Double topologyWorkerMaxHeapSize;
    //topology priority
    private Integer topologyPriority;
    //when topology was launched
    private int launchTime;

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDetails.class);

    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers) {
        this(topologyId, topologyConf, topology,  numWorkers,  null, 0);
    }

    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology,
                           int numWorkers, Map<ExecutorDetails, String> executorToComponents) {
        this(topologyId, topologyConf, topology,  numWorkers,  executorToComponents, 0);
    }

    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology,
                           int numWorkers, Map<ExecutorDetails, String> executorToComponents, int launchTime) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;
        this.executorToComponent = new HashMap<>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
        if (this.topology != null) {
            this.initResourceList();
        }
        this.initConfigs();
        this.launchTime = launchTime;
    }

    public String getId() {
        return this.topologyId;
    }

    public String getName() {
        return (String) this.topologyConf.get(Config.TOPOLOGY_NAME);
    }

    public Map getConf() {
        return this.topologyConf;
    }

    public int getNumWorkers() {
        return this.numWorkers;
    }

    public StormTopology getTopology() {
        return topology;
    }

    public Map<ExecutorDetails, String> getExecutorToComponent() {
        return this.executorToComponent;
    }

    public Map<ExecutorDetails, String> selectExecutorToComponent(Collection<ExecutorDetails> executors) {
        Map<ExecutorDetails, String> ret = new HashMap<>(executors.size());
        for (ExecutorDetails executor : executors) {
            String compId = this.executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }

        return ret;
    }

    public Collection<ExecutorDetails> getExecutors() {
        return this.executorToComponent.keySet();
    }

    private void initResourceList() {
        this.resourceList = new HashMap<>();
        // Extract bolt memory info
        if (this.topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : this.topology.get_bolts().entrySet()) {
                //the json_conf is populated by TopologyBuilder (e.g. boltDeclarer.setMemoryLoad)
                Map<String, Double> topology_resources = ResourceUtils.parseResources(bolt
                        .getValue().get_common().get_json_conf());
                ResourceUtils.checkIntialization(topology_resources, bolt.getValue().toString(), this.topologyConf);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : this.executorToComponent.entrySet()) {
                    if (bolt.getKey().equals(anExecutorToComponent.getValue())) {
                        this.resourceList.put(anExecutorToComponent.getKey(), topology_resources);
                    }
                }
            }
        }
        // Extract spout memory info
        if (this.topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : this.topology.get_spouts().entrySet()) {
                Map<String, Double> topology_resources = ResourceUtils.parseResources(spout
                        .getValue().get_common().get_json_conf());
                ResourceUtils.checkIntialization(topology_resources, spout.getValue().toString(), this.topologyConf);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : this.executorToComponent.entrySet()) {
                    if (spout.getKey().equals(anExecutorToComponent.getValue())) {
                        this.resourceList.put(anExecutorToComponent.getKey(), topology_resources);
                    }
                }
            }
        } else {
            LOG.warn("Topology " + this.topologyId + " does not seem to have any spouts!");
        }
        //schedule tasks that are not part of components returned from topology.get_spout or topology.getbolt (AKA sys tasks most specifically __acker tasks)
        for(ExecutorDetails exec : this.getExecutors()) {
            if (!this.resourceList.containsKey(exec)) {
                LOG.debug(
                        "Scheduling {} {} with memory requirement as 'on heap' - {} and 'off heap' - {} and CPU requirement as {}",
                        this.getExecutorToComponent().get(exec),
                        exec,
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB),
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB),
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
                this.addDefaultResforExec(exec);
            } 
        }
    }

    private List<ExecutorDetails> componentToExecs(String comp) {
        List<ExecutorDetails> execs = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, String> entry : this.executorToComponent.entrySet()) {
            if (entry.getValue().equals(comp)) {
                execs.add(entry.getKey());
            }
        }
        return execs;
    }

    /**
     * Returns a representation of the non-system components of the topology graph
     * Each Component object in the returning map is populated with the list of its
     * parents, children and execs assigned to that component.
     * @return a map of components
     */
    public Map<String, Component> getComponents() {
        Map<String, Component> all_comp = new HashMap<>();

        StormTopology storm_topo = this.topology;
        // spouts
        if (storm_topo.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spoutEntry : storm_topo
                    .get_spouts().entrySet()) {
                if (!Utils.isSystemId(spoutEntry.getKey())) {
                    Component newComp;
                    if (all_comp.containsKey(spoutEntry.getKey())) {
                        newComp = all_comp.get(spoutEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                    } else {
                        newComp = new Component(spoutEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                        all_comp.put(spoutEntry.getKey(), newComp);
                    }
                    newComp.type = Component.ComponentType.SPOUT;

                    for (Map.Entry<GlobalStreamId, Grouping> spoutInput : spoutEntry
                            .getValue().get_common().get_inputs()
                            .entrySet()) {
                        newComp.parents.add(spoutInput.getKey()
                                .get_componentId());
                        if (!all_comp.containsKey(spoutInput
                                .getKey().get_componentId())) {
                            all_comp.put(spoutInput.getKey()
                                            .get_componentId(),
                                    new Component(spoutInput.getKey()
                                            .get_componentId()));
                        }
                        all_comp.get(spoutInput.getKey()
                                .get_componentId()).children.add(spoutEntry
                                .getKey());
                    }
                }
            }
        }
        // bolts
        if (storm_topo.get_bolts() != null) {
            for (Map.Entry<String, Bolt> boltEntry : storm_topo.get_bolts()
                    .entrySet()) {
                if (!Utils.isSystemId(boltEntry.getKey())) {
                    Component newComp;
                    if (all_comp.containsKey(boltEntry.getKey())) {
                        newComp = all_comp.get(boltEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                    } else {
                        newComp = new Component(boltEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                        all_comp.put(boltEntry.getKey(), newComp);
                    }
                    newComp.type = Component.ComponentType.BOLT;

                    for (Map.Entry<GlobalStreamId, Grouping> boltInput : boltEntry
                            .getValue().get_common().get_inputs()
                            .entrySet()) {
                        newComp.parents.add(boltInput.getKey()
                                .get_componentId());
                        if (!all_comp.containsKey(boltInput
                                .getKey().get_componentId())) {
                            all_comp.put(boltInput.getKey()
                                            .get_componentId(),
                                    new Component(boltInput.getKey()
                                            .get_componentId()));
                        }
                        all_comp.get(boltInput.getKey()
                                .get_componentId()).children.add(boltEntry
                                .getKey());
                    }
                }
            }
        }
        return all_comp;
    }

    /**
     * Gets the on heap memory requirement for a
     * certain task within a topology
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of on heap memory
     * requirement for this exec in topology topoId.
     */
    public Double getOnHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            ret = this.resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        }
        return ret;
    }

    /**
     * Gets the off heap memory requirement for a
     * certain task within a topology
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of off heap memory
     * requirement for this exec in topology topoId.
     */
    public Double getOffHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            ret = this.resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        }
        return ret;
    }

    /**
     * Gets the total memory requirement for a task
     * @param exec the executor the inquiry is concerning.
     * @return Double the total memory requirement
     *  for this exec in topology topoId.
     */
    public Double getTotalMemReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return getOffHeapMemoryRequirement(exec)
                    + getOnHeapMemoryRequirement(exec);
        }
        return null;
    }

    /**
     * Gets the total memory resource list for a
     * set of tasks that is part of a topology.
     * @return Map<ExecutorDetails, Double> a map of the total memory requirement
     *  for all tasks in topology topoId.
     */
    public Map<ExecutorDetails, Double> getTotalMemoryResourceList() {
        Map<ExecutorDetails, Double> ret = new HashMap<>();
        for (ExecutorDetails exec : this.resourceList.keySet()) {
            ret.put(exec, getTotalMemReqTask(exec));
        }
        return ret;
    }

    /**
     * Get the total CPU requirement for executor
     * @return Double the total about of cpu requirement for executor
     */
    public Double getTotalCpuReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return this.resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
        }
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total on-heap memory requested for this topology
     */
    public Double getTotalRequestedMemOnHeap() {
        Double total_memonheap = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double exec_mem = getOnHeapMemoryRequirement(exec);
            if (exec_mem != null) {
                total_memonheap += exec_mem;
            }
        }
        return total_memonheap;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total off-heap memory requested for this topology
     */
    public Double getTotalRequestedMemOffHeap() {
        Double total_memoffheap = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double exec_mem = getOffHeapMemoryRequirement(exec);
            if (exec_mem != null) {
                total_memoffheap += exec_mem;
            }
        }
        return total_memoffheap;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total cpu requested for this topology
     */
    public Double getTotalRequestedCpu() {
        Double total_cpu = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double exec_cpu = getTotalCpuReqTask(exec);
            if (exec_cpu != null) {
                total_cpu += exec_cpu;
            }
        }
        return total_cpu;
    }

    /**
     * get the resources requirements for a executor
     * @param exec
     * @return a map containing the resource requirements for this exec
     */
    public Map<String, Double> getTaskResourceReqList(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return this.resourceList.get(exec);
        }
        return null;
    }

    /**
     * Checks if a executor is part of this topology
     * @return Boolean whether or not a certain ExecutorDetail is included in the resourceList.
     */
    public boolean hasExecInTopo(ExecutorDetails exec) {
        return this.resourceList != null && this.resourceList.containsKey(exec);
    }

    /**
     * add resource requirements for a executor
     */
    public void addResourcesForExec(ExecutorDetails exec, Map<String, Double> resourceList) {
        if (hasExecInTopo(exec)) {
            LOG.warn("Executor {} already exists...ResourceList: {}", exec, getTaskResourceReqList(exec));
            return;
        }
        this.resourceList.put(exec, resourceList);
    }

    /**
     * Add default resource requirements for a executor
     */
    private void addDefaultResforExec(ExecutorDetails exec) {
        Double topologyComponentCpuPcorePercent = Utils.getDouble(this.topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
        Double topologyComponentResourcesOffheapMemoryMb = Utils.getDouble(this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
        Double topologyComponentResourcesOnheapMemoryMb = Utils.getDouble(this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);

        assert topologyComponentCpuPcorePercent != null;
        assert topologyComponentResourcesOffheapMemoryMb != null;
        assert topologyComponentResourcesOnheapMemoryMb != null;

        Map<String, Double> defaultResourceList = new HashMap<>();
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topologyComponentCpuPcorePercent);
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topologyComponentResourcesOffheapMemoryMb);
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topologyComponentResourcesOnheapMemoryMb);
        LOG.debug("Scheduling Executor: {} with memory requirement as onHeap: {} - offHeap: {} " +
                        "and CPU requirement: {}",
                exec, this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB),
                this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB),
                this.topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        addResourcesForExec(exec, defaultResourceList);
    }

    /**
     * initializes member variables
     */
    private void initConfigs() {
        this.topologyWorkerMaxHeapSize = Utils.getDouble(this.topologyConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB), null);
        this.topologyPriority = Utils.getInt(this.topologyConf.get(Config.TOPOLOGY_PRIORITY), null);

        assert this.topologyWorkerMaxHeapSize != null;
        assert this.topologyPriority != null;
    }

    /**
     * Get the max heap size for a worker used by this topology
     * @return the worker max heap size
     */
    public Double getTopologyWorkerMaxHeapSize() {
        return this.topologyWorkerMaxHeapSize;
    }

    /**
     * Get the user that submitted this topology
     */
    public String getTopologySubmitter() {
        String user = (String) this.topologyConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        if (user == null || user.equals("")) {
            LOG.debug("Topology {} submitted by anonymous user", this.getName());
            user = System.getProperty("user.name");
        }
        return user;
    }

    /**
     * get teh priority of this topology
     */
    public int getTopologyPriority() {
       return this.topologyPriority;
    }

    /**
     * Get the timestamp of when this topology was launched
     */
    public int getLaunchTime() {
        return this.launchTime;
    }

    /**
     * Get how long this topology has been executing
     */
    public int getUpTime() {
        return Time.currentTimeSecs() - this.launchTime;
    }

    @Override
    public String toString() {
        return "Name: " + this.getName() + " id: " + this.getId() + " Priority: " + this.getTopologyPriority()
                + " Uptime: " + this.getUpTime() + " CPU: " + this.getTotalRequestedCpu()
                + " Memory: " + (this.getTotalRequestedMemOffHeap() + this.getTotalRequestedMemOnHeap());
    }

    @Override
    public int hashCode() {
        return this.topologyId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopologyDetails)) {
            return false;
        }
        return (this.topologyId.equals(((TopologyDetails) o).getId()));
    }
}
