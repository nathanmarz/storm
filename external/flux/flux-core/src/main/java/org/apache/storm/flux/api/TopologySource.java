/*
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
package org.apache.storm.flux.api;


import org.apache.storm.generated.StormTopology;

import java.util.Map;

/**
 * Marker interface for objects that can produce `StormTopology` objects.
 *
 * If a `topology-source` class implements the `getTopology()` method, Flux will
 * call that method. Otherwise, it will introspect the given class and look for a
 * similar method that produces a `StormTopology` instance.
 *
 * Note that it is not strictly necessary for a class to implement this interface.
 * If a class defines a method with a similar signature, Flux should be able to find
 * and invoke it.
 *
 */
public interface TopologySource {
    public StormTopology getTopology(Map<String, Object> config);
}
