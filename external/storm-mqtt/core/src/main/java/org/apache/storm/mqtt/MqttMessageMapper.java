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
package org.apache.storm.mqtt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;

/**
 * Represents an object that can be converted to a Storm Tuple from an AckableMessage,
 * given a MQTT Topic Name and a byte array payload.
 */
public interface MqttMessageMapper extends Serializable {
    /**
     * Convert a `MqttMessage` to a set of Values that can be emitted as a Storm Tuple.
     *
     * @param message An MQTT Message.
     * @return Values representing a Storm Tuple.
     */
    Values toValues(MqttMessage message);

    /**
     * Returns the list of output fields this Mapper produces.
     *
     * @return the list of output fields this mapper produces.
     */
    Fields outputFields();
}
