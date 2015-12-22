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
package backtype.storm.topology;

import backtype.storm.state.State;

/**
 * <p>
 * Common methods for stateful components in the topology.
 * </p>
 * A stateful component is one that has state (e.g. the result of some computation in a bolt)
 * and wants the framework to manage its state.
 */
public interface IStatefulComponent<T extends State> extends IComponent {
    /**
     * This method is invoked by the framework with the previously
     * saved state of the component. This is invoked after prepare but before
     * the component starts processing tuples.
     *
     * @param state the previously saved state of the component.
     */
    void initState(T state);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework commits its state.
     */
    void preCommit(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework prepares its state.
     */
    void prePrepare(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework rolls back the prepared state.
     */
    void preRollback();
}
