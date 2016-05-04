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

import java.util.Map;

/**
 * Base BeanProvider implementation.
 */
public abstract class BaseBeanFactory<T> implements BeanFactory<T> {

    protected WorkerCtx context;

    protected volatile T instance;
    /**
     * {@inheritDoc}
     */
    @Override
    public void setStormContext(WorkerCtx context) {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized T get(Map<String, Object> stormConf) {
        if( instance != null) return instance;
        return instance = make(stormConf);
    }
    /**
     * Return a new instance of T.
     */
    protected abstract T make(final Map<String, Object> stormConf);
    /**
     * {@inheritDoc}
     */
    @Override
    public BeanFactory<T> newInstance() {
        Class<? extends BaseBeanFactory> clazz = this.getClass();
        try {
            BaseBeanFactory factory = clazz.newInstance();
            factory.setStormContext(this.context);
            return factory;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Cannot create a new instance of " + clazz.getSimpleName(), e);
        }
    }
}