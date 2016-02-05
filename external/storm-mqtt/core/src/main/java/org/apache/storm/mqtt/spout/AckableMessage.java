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
package org.apache.storm.mqtt.spout;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.storm.mqtt.MqttMessage;

/**
 * Represents an MQTT Message consisting of a topic string (e.g. "/users/ptgoetz/office/thermostat")
 * and a byte array message/payload.
 *
 */
class AckableMessage {
    private String topic;
    private byte[] message;
    private Runnable ack;

    AckableMessage(String topic, byte[] message, Runnable ack){
        this.topic = topic;
        this.message = message;
        this.ack = ack;
    }

    public MqttMessage getMessage(){
        return new MqttMessage(this.topic, this.message);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 123)
                .append(this.topic)
                .append(this.message)
                .toHashCode();
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AckableMessage tm = (AckableMessage)obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(this.topic, tm.topic)
                .append(this.message, tm.message)
                .isEquals();
    }

    Runnable ack(){
        return this.ack;
    }
}