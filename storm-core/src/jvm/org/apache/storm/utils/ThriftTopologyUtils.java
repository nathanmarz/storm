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
package org.apache.storm.utils;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ThriftTopologyUtils {
    public static boolean isWorkerHook(StormTopology._Fields f) {
        return f.equals(StormTopology._Fields.WORKER_HOOKS);
    }

    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<String>();
        for(StormTopology._Fields f: StormTopology.metaDataMap.keySet()) {
            if(StormTopology.metaDataMap.get(f).valueMetaData.type == org.apache.thrift.protocol.TType.MAP) {
                Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
                ret.addAll(componentMap.keySet());
            }
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        for(StormTopology._Fields f: StormTopology.metaDataMap.keySet()) {
            if(StormTopology.metaDataMap.get(f).valueMetaData.type == org.apache.thrift.protocol.TType.MAP) {
                Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
                if(componentMap.containsKey(componentId)) {
                    Object component = componentMap.get(componentId);
                    if(component instanceof Bolt) {
                        return ((Bolt) component).get_common();
                    }
                    if(component instanceof SpoutSpec) {
                        return ((SpoutSpec) component).get_common();
                    }
                    if(component instanceof StateSpoutSpec) {
                        return ((StateSpoutSpec) component).get_common();
                    }
                    throw new RuntimeException("Unreachable code! No get_common conversion for component " + component);
                }
            }
        }
        throw new IllegalArgumentException("Could not find component common for " + componentId);
    }
}
