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
package org.apache.storm.cassandra.context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple class used to register singletons within a storm worker.
 */
public class WorkerCtx implements Serializable {

    private static final ConcurrentMap<Class, BeanFactory<?>> workerCtx = new ConcurrentHashMap<>();

    private Map<Class, BeanFactory<?>> componentCtx = new HashMap<>();

    /**
     * Creates a new {@link WorkerCtx} instance.
     */
    public WorkerCtx() {
        super();
    }

    /**
     * Register a bean provider for a specified type.
     */
    public <T> void register(Class<T> clazz, BeanFactory<T> provider) {
        componentCtx.put(clazz, provider);
    }

    /**
     * Return an instance provider of the specified type.
     * @throws RuntimeException if no bean provider can be resolve for the given class.
     * @return
     */
    protected <T> BeanFactory<T> getBeanfactory(Class<T> clazz) {
        BeanFactory<T> factory = (BeanFactory<T>) this.componentCtx.get(clazz);
        if( factory == null) throw new RuntimeException("Cannot resolve bean factory for class : " + clazz.getCanonicalName());
        factory.setStormContext(this);
        return factory;
    }

    /**
     * Return an instance, which is shared (within a Worker), of the specified type.
     * @return
     */
    public <T, K, V> T getWorkerBean(Class<T> clazz, Map<K, V> stormConf) {
        return getWorkerBean(clazz, stormConf,false);
    }

    /**
     * Return an instance, which is shared (within a Worker), of the specified type.
     *
     * @param clazz the class of the bean.
     * @param stormConf the storm configuration
     * @param force if <code>true</code>= create a new instance even if one already exist.
     *
     * @return a instance of type {@link T}.
     */
    public <T, K, V> T getWorkerBean(Class<T> clazz, Map<K, V> stormConf, boolean force) {
        if( force ) workerCtx.remove(clazz);
        BeanFactory<T> factory = (BeanFactory<T>)  this.workerCtx.get(clazz);
        if( factory == null) {
            BeanFactory<T> instance = getBeanfactory(clazz).newInstance();
            workerCtx.putIfAbsent(clazz, instance);
            factory = (BeanFactory<T>) this.workerCtx.get(clazz);
        }
        return factory.get((Map<String, Object>)stormConf);
    }
}