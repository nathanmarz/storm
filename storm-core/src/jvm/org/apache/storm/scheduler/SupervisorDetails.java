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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorDetails {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorDetails.class);

    String id;
    /**
     * hostname of this supervisor
     */
    String host;
    Object meta;
    /**
     * meta data configured for this supervisor
     */
    Object schedulerMeta;
    /**
     * all the ports of the supervisor
     */
    Set<Integer> allPorts;
    /**
     * Map containing a manifest of resources for the node the supervisor resides
     */
    private Map<String, Double> _total_resources;

    public SupervisorDetails(String id, String host, Object meta, Object schedulerMeta,
                             Collection<Number> allPorts, Map<String, Double> total_resources){

        this.id = id;
        this.host = host;
        this.meta = meta;
        this.schedulerMeta = schedulerMeta;
        if(allPorts!=null) {
            setAllPorts(allPorts);
        } else {
            this.allPorts = new HashSet<>();
        }
        this._total_resources = total_resources;
        LOG.debug("Creating a new supervisor ({}-{}) with resources: {}", this.host, this.id, total_resources);
    }

    public SupervisorDetails(String id, Object meta){
        this(id, null, meta, null, null, null);
    }

    public SupervisorDetails(String id, Object meta, Map<String, Double> total_resources) {
        this(id, null, meta, null, null, total_resources);
    }

    public SupervisorDetails(String id, Object meta, Collection<Number> allPorts){
        this(id, null, meta, null, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<Number> allPorts) {
        this(id, host, null, schedulerMeta, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta,
                             Collection<Number> allPorts, Map<String, Double> total_resources) {
        this(id, host, null, schedulerMeta, allPorts, total_resources);
    }

    private void setAllPorts(Collection<Number> allPorts) {
        this.allPorts = new HashSet<>();
        if(allPorts!=null) {
            for(Number n: allPorts) {
                this.allPorts.add(n.intValue());
            }
        }
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Object getMeta() {
        return meta;
    }
    
    public Set<Integer> getAllPorts() {
        return allPorts;
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }

    private Double getTotalResource(String type) {
        return this._total_resources.get(type);
    }

    public Double getTotalMemory() {
        Double totalMemory = getTotalResource(Config.SUPERVISOR_MEMORY_CAPACITY_MB);
        assert totalMemory != null;
        return totalMemory;
    }

    public Double getTotalCPU() {
        Double totalCPU = getTotalResource(Config.SUPERVISOR_CPU_CAPACITY);
        assert totalCPU != null;
        return totalCPU;
    }
}
