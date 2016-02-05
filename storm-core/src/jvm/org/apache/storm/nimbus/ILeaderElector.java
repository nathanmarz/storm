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

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * The interface for leader election.
 */
public interface ILeaderElector extends Closeable {

    /**
     * Method guaranteed to be called as part of initialization of leader elector instance.
     * @param conf configuration
     */
    void prepare(Map conf);

    /**
     * queue up for leadership lock. The call returns immediately and the caller must
     * check isLeader() to perform any leadership action. This method can be called
     * multiple times so it needs to be idempotent.
     */
    void addToLeaderLockQueue() throws Exception;

    /**
     * Removes the caller from the leader lock queue. If the caller is leader
     * also releases the lock. This method can be called multiple times so it needs
     * to be idempotent.
     */
    void removeFromLeaderLockQueue() throws Exception;

    /**
     *
     * @return true if the caller currently has the leader lock.
     */
    boolean isLeader() throws Exception;

    /**
     *
     * @return the current leader's address , may return null if no one has the lock.
     */
    NimbusInfo getLeader();

    /**
     *
     * @return list of current nimbus addresses, includes leader.
     */
    List<NimbusInfo> getAllNimbuses()throws Exception;

    /**
     * Method called to allow for cleanup. once close this object can not be reused.
     */
    @Override
    void close();
}

