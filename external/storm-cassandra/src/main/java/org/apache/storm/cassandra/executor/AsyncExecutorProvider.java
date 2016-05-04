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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.executor;

import com.datastax.driver.core.Session;

/**
 * This class must be used to obtain a single instance of {@link AsyncExecutor} per storm executor.
 */
public class AsyncExecutorProvider {

    private static final ThreadLocal<AsyncExecutor> localAsyncExecutor = new ThreadLocal<>();

    /**
     * Returns a new {@link AsyncExecutor} per storm executor.
     */
    public static <T> AsyncExecutor getLocal(Session session, AsyncResultHandler<T> handler) {
        AsyncExecutor<T> executor = localAsyncExecutor.get();
        if( executor == null ) {
            localAsyncExecutor.set(executor = new AsyncExecutor<>(session, handler));
        }
        return executor;
    }
}
