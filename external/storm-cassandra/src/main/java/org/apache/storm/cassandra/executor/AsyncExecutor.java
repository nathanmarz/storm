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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service to asynchronously executes cassandra statements.
 */
public class AsyncExecutor<T> implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(AsyncExecutor.class);

    protected Session session;

    protected ExecutorService executorService;

    protected AsyncResultHandler<T> handler;

    private AtomicInteger pending = new AtomicInteger();

    /**
     * Creates a new {@link AsyncExecutor} instance.
     */
    protected AsyncExecutor(Session session, AsyncResultHandler<T> handler) {
        this(session, newSingleThreadExecutor(), handler);
    }

    /**
     * Creates a new {@link AsyncExecutor} instance.
     *
     * @param session The cassandra session.
     * @param executorService The executor service responsible to execute handler.
     */
    private AsyncExecutor(Session session, ExecutorService executorService, AsyncResultHandler<T> handler) {
        this.session   = session;
        this.executorService = executorService;
        this.handler = handler;
    }

    protected static ExecutorService newSingleThreadExecutor() {
        return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("cassandra-async-handler-%d").build());
    }

    /**
     * Asynchronously executes all statements associated to the specified input.
     * The input will be passed to handler#onSuccess once all queries succeed or to handler#onFailure if any one of them fails.
     */
    public List<SettableFuture<T>> execAsync(List<Statement> statements, final T input) {

        List<SettableFuture<T>> settableFutures = new ArrayList<>(statements.size());

        for(Statement s : statements)
            settableFutures.add(execAsync(s, input, AsyncResultHandler.NO_OP_HANDLER));

        ListenableFuture<List<T>> allAsList = Futures.allAsList(settableFutures);
        Futures.addCallback(allAsList, new FutureCallback<List<T>>(){
            @Override
            public void onSuccess(List<T> inputs) {
                handler.success(input);
            }

            @Override
            public void onFailure(Throwable t) {
                handler.failure(t, input);
            }
        }, executorService);
        return settableFutures;
    }

    /**
     * Asynchronously executes the specified batch statement. Inputs will be passed to
     * the {@link #handler} once query succeed or failed.
     */
    public SettableFuture<T> execAsync(final Statement statement, final T inputs) {
        return execAsync(statement, inputs, handler);
    }
    /**
     * Asynchronously executes the specified batch statement. Inputs will be passed to
     * the {@link #handler} once query succeed or failed.
     */
    public SettableFuture<T> execAsync(final Statement statement, final T inputs, final AsyncResultHandler<T> handler) {
        final SettableFuture<T> settableFuture = SettableFuture.create();
        pending.incrementAndGet();
        ResultSetFuture future = session.executeAsync(statement);
        Futures.addCallback(future, new FutureCallback<ResultSet>() {
            public void release() {
                pending.decrementAndGet();
            }

            @Override
            public void onSuccess(ResultSet result) {
                release();
                settableFuture.set(inputs);
                handler.success(inputs);
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error(String.format("Failed to execute statement '%s' ", statement), t);
                release();
                settableFuture.setException(t);
                handler.failure(t, inputs);
            }
        }, executorService);
        return settableFuture;
    }

    /**
     * Returns the number of currently executed tasks which are not yet completed.
     */
    public int getPendingTasksSize() {
        return this.pending.intValue();
    }

    public void shutdown( ) {
        if( ! executorService.isShutdown() ) {
            LOG.info("shutting down async handler executor");
            this.executorService.shutdownNow();
        }
    }
}
