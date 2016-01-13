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

import org.apache.storm.topology.IStatefulBolt;

/**
 * The state of the component that is either managed by the framework (e.g in case of {@link IStatefulBolt})
 * or managed by the the individual components themselves.
 */
public interface State {
    /**
     * Invoked by the framework to prepare a transaction for commit. It should be possible
     * to commit the prepared state later.
     * <p>
     * The same txid can be prepared again, but the next txid cannot be prepared
     * when previous one is not yet committed.
     * </p>
     *
     * @param txid the transaction id
     */
    void prepareCommit(long txid);

    /**
     * Commit a previously prepared transaction. It should be possible to retrieve a committed state later.
     *
     * @param txid the transaction id
     */
    void commit(long txid);

    /**
     * Persist the current state. This is used when the component manages the state.
     */
    void commit();

    /**
     * Rollback a prepared transaction to the previously committed state.
     */
    void rollback();
}
