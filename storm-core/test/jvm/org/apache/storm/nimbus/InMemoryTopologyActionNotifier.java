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
package org.apache.storm.nimbus;

import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;

public class InMemoryTopologyActionNotifier implements  ITopologyActionNotifierPlugin {

    //static to ensure eventhough the class is created using reflection we can still get
    //the topology to actions
    private static final Map<String, LinkedList<String>> topologyToActions = new HashMap<>();


    @Override
    public void prepare(Map StormConf) {
        //no-op
    }

    @Override
    public synchronized void notify(String topologyName, String action) {
        if(!topologyToActions.containsKey(topologyName)) {
            topologyToActions.put(topologyName, new LinkedList<String>());
        }
        topologyToActions.get(topologyName).addLast(action);
    }

    public List<String> getTopologyActions(String topologyName) {
        return topologyToActions.get(topologyName);
    }

    @Override
    public void cleanup() {
        //no-op
    }
}
