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
package org.apache.storm.state;

import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * Used by the {@link StateFactory} to create a new state instances.
 */
public interface StateProvider {
    /**
     * Returns a new state instance. Each state belongs unique namespace which is typically
     * the componentid-task of the task, so that each task can have its own unique state.
     *
     * @param namespace a namespace of the state
     * @param stormConf the storm topology configuration
     * @param context   the {@link TopologyContext}
     * @return a previously saved state if one exists otherwise a newly initialized state.
     */
    State newState(String namespace, Map stormConf, TopologyContext context);
}
