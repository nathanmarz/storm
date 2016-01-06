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
package org.apache.storm.mqtt.common;


import org.apache.storm.mqtt.MqttLogger;
import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.ssl.KeyStoreLoader;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class MqttPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(MqttPublisher.class);

    private MqttOptions options;
    private transient BlockingConnection connection;
    private KeyStoreLoader keyStoreLoader;
    private QoS qos;
    private boolean retain = false;


    public MqttPublisher(MqttOptions options){
        this(options, null, false);
    }

    public MqttPublisher(MqttOptions options, boolean retain){
        this(options, null, retain);
    }

    public MqttPublisher(MqttOptions options, KeyStoreLoader keyStoreLoader, boolean retain){
        this.retain = retain;
        this.options = options;
        this.keyStoreLoader = keyStoreLoader;
        SslUtils.checkSslConfig(this.options.getUrl(), keyStoreLoader);
        this.qos = MqttUtils.qosFromInt(this.options.getQos());
    }

    public void publish(MqttMessage message) throws Exception {
        this.connection.publish(message.getTopic(), message.getMessage(), this.qos, this.retain);
    }

    public void connectMqtt(String clientId) throws Exception {
        MQTT client = MqttUtils.configureClient(this.options, clientId, this.keyStoreLoader);
        this.connection = client.blockingConnection();
        this.connection.connect();
    }
}
