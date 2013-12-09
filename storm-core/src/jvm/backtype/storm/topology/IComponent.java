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

import java.io.Serializable;
import java.util.Map;

/**
 * Common methods for all possible components in a topology. This interface is used
 * when defining topologies using the Java API. 
 */
public interface IComponent extends Serializable {

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);

    /**
     * Declare configuration specific to this component. Only a subset of the "topology.*" configs can
     * be overridden. The component configuration can be further overridden when constructing the 
     * topology using {@link TopologyBuilder}
     *
     */
    Map<String, Object> getComponentConfiguration();

}
