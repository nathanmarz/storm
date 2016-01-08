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
import org.apache.storm.mqtt.ssl.KeyStoreLoader;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class MqttUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MqttUtils.class);

    private MqttUtils(){}

    public static QoS qosFromInt(int i){
        QoS qos = null;
        switch(i) {
            case 0:
                qos = QoS.AT_MOST_ONCE;
                break;
            case 1:
                qos = QoS.AT_LEAST_ONCE;
                break;
            case 2:
                qos = QoS.EXACTLY_ONCE;
                break;
            default:
                throw new IllegalArgumentException(i + "is not a valid MQTT QoS.");
        }
        return qos;
    }


    public static MQTT configureClient(MqttOptions options, String clientId, KeyStoreLoader keyStoreLoader)
            throws Exception{

        MQTT client = new MQTT();
        URI uri = URI.create(options.getUrl());

        client.setHost(uri);
        if(!uri.getScheme().toLowerCase().equals("tcp")){
            client.setSslContext(SslUtils.sslContext(uri.getScheme(), keyStoreLoader));
        }
        client.setClientId(clientId);
        LOG.info("MQTT ClientID: {}", client.getClientId().toString());
        client.setCleanSession(options.isCleanConnection());

        client.setReconnectDelay(options.getReconnectDelay());
        client.setReconnectDelayMax(options.getReconnectDelayMax());
        client.setReconnectBackOffMultiplier(options.getReconnectBackOffMultiplier());
        client.setConnectAttemptsMax(options.getConnectAttemptsMax());
        client.setReconnectAttemptsMax(options.getReconnectAttemptsMax());


        client.setUserName(options.getUserName());
        client.setPassword(options.getPassword());
        client.setTracer(new MqttLogger());

        if(options.getWillTopic() != null && options.getWillPayload() != null){
            QoS qos = MqttUtils.qosFromInt(options.getWillQos());
            client.setWillQos(qos);
            client.setWillTopic(options.getWillTopic());
            client.setWillMessage(options.getWillPayload());
            client.setWillRetain(options.getWillRetain());
        }
        return client;
    }
}
