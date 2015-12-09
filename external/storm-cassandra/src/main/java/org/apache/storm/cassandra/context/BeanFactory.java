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
import java.util.Map;

/**
 * Simple interface used for providing services based on the storm configuration.
 */
public interface BeanFactory<T> extends Serializable {

    /**
     * Sets the storm context.
     * @param context
     */
    public void setStormContext(WorkerCtx context);

    /**
     * Return an instance, which may be shared or independent, of the specified type.
     * @param stormConf The storm configuration
     * @return
     */
    T get(Map<String, Object> stormConf);


    /**
     * Returns a new copy if this factory.
     * @return a new {@link BeanFactory} instance.
     */
    public BeanFactory<T> newInstance();
}