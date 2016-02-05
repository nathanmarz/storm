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
package org.apache.storm.mqtt.examples;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a topic name: "users/{user}/{location}/{deviceId}"
 * and a payload of "{temperature}/{humidity}"
 * emits a tuple containing user(String), deviceId(String), location(String), temperature(float), humidity(float)
 *
 */
public class CustomMessageMapper implements MqttMessageMapper {
    private static final Logger LOG = LoggerFactory.getLogger(CustomMessageMapper.class);


    public Values toValues(MqttMessage message) {
        String topic = message.getTopic();
        String[] topicElements = topic.split("/");
        String[] payloadElements = new String(message.getMessage()).split("/");

        return new Values(topicElements[2], topicElements[4], topicElements[3], Float.parseFloat(payloadElements[0]),
                Float.parseFloat(payloadElements[1]));
    }

    public Fields outputFields() {
        return new Fields("user", "deviceId", "location", "temperature", "humidity");
    }
}
