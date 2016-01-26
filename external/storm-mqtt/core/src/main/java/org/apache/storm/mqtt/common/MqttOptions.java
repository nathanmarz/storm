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

import java.io.Serializable;
import java.util.List;

/**
 * MQTT Configuration Options
 */
public class MqttOptions implements Serializable {
    private String url = "tcp://localhost:1883";
    private List<String> topics = null;
    private boolean cleanConnection = false;

    private String willTopic;
    private String willPayload;
    private int willQos = 1;
    private boolean willRetain = false;

    private long reconnectDelay = 10;
    private long reconnectDelayMax = 30*1000;
    private double reconnectBackOffMultiplier = 2.0f;
    private long reconnectAttemptsMax = -1;
    private long connectAttemptsMax = -1;

    private String userName = "";
    private String password = "";

    private int qos = 1;

    public String getUrl() {
        return url;
    }

    /**
     * Sets the url for connecting to the MQTT broker.
     *
     * Default: `tcp://localhost:1883'
     * @param url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    public List<String> getTopics() {
        return topics;
    }

    /**
     * A list of MQTT topics to subscribe to.
     *
     * @param topics
     */
    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public boolean isCleanConnection() {
        return cleanConnection;
    }

    /**
     * Set to false if you want the MQTT server to persist topic subscriptions and ack positions across client sessions.
     * Defaults to false.
     *
     * @param cleanConnection
     */
    public void setCleanConnection(boolean cleanConnection) {
        this.cleanConnection = cleanConnection;
    }

    public String getWillTopic() {
        return willTopic;
    }

    /**
     * If set the server will publish the client's Will message to the specified topics if the client has an unexpected
     * disconnection.
     *
     * @param willTopic
     */
    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public String getWillPayload() {
        return willPayload;
    }

    /**
     * The Will message to send. Defaults to a zero length message.
     *
     * @param willPayload
     */
    public void setWillPayload(String willPayload) {
        this.willPayload = willPayload;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    /**
     * How long to wait in ms before the first reconnect attempt. Defaults to 10.
     *
     * @param reconnectDelay
     */
    public void setReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public long getReconnectDelayMax() {
        return reconnectDelayMax;
    }

    /**
     * The maximum amount of time in ms to wait between reconnect attempts. Defaults to 30,000.
     *
     * @param reconnectDelayMax
     */
    public void setReconnectDelayMax(long reconnectDelayMax) {
        this.reconnectDelayMax = reconnectDelayMax;
    }

    public double getReconnectBackOffMultiplier() {
        return reconnectBackOffMultiplier;
    }

    /**
     * The Exponential backoff be used between reconnect attempts. Set to 1 to disable exponential backoff. Defaults to
     * 2.
     *
     * @param reconnectBackOffMultiplier
     */
    public void setReconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
        this.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
    }

    public long getReconnectAttemptsMax() {
        return reconnectAttemptsMax;
    }

    /**
     * The maximum number of reconnect attempts before an error is reported back to the client after a server
     * connection had previously been established. Set to -1 to use unlimited attempts. Defaults to -1.
     *
     * @param reconnectAttemptsMax
     */
    public void setReconnectAttemptsMax(long reconnectAttemptsMax) {
        this.reconnectAttemptsMax = reconnectAttemptsMax;
    }

    public long getConnectAttemptsMax() {
        return connectAttemptsMax;
    }

    /**
     * The maximum number of reconnect attempts before an error is reported back to the client on the first attempt by
     * the client to connect to a server. Set to -1 to use unlimited attempts. Defaults to -1.
     *
     * @param connectAttemptsMax
     */
    public void setConnectAttemptsMax(long connectAttemptsMax) {
        this.connectAttemptsMax = connectAttemptsMax;
    }

    public String getUserName() {
        return userName;
    }

    /**
     * The username for authenticated sessions.
     *
     * @param userName
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    /**
     * The password for authenticated sessions.
     * @param password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public int getQos(){
        return this.qos;
    }

    /**
     * Sets the quality of service to use for MQTT messages. Defaults to 1 (at least once).
     * @param qos
     */
    public void setQos(int qos){
        if(qos < 0 || qos > 2){
            throw new IllegalArgumentException("MQTT QoS must be >= 0 and <= 2");
        }
        this.qos = qos;
    }

    public int getWillQos(){
        return this.willQos;
    }

    /**
     * Sets the quality of service to use for the MQTT Will message. Defaults to 1 (at least once).
     *
     * @param qos
     */
    public void setWillQos(int qos){
        if(qos < 0 || qos > 2){
            throw new IllegalArgumentException("MQTT Will QoS must be >= 0 and <= 2");
        }
        this.willQos = qos;
    }

    public boolean getWillRetain(){
        return this.willRetain;
    }

    /**
     * Set to true if you want the Will message to be published with the retain option.
     * @param retain
     */
    public void setWillRetain(boolean retain){
        this.willRetain = retain;
    }

    public static class Builder {
        private MqttOptions options = new MqttOptions();

        public Builder url(String url) {
            this.options.url = url;
            return this;
        }


        public Builder topics(List<String> topics) {
            this.options.topics = topics;
            return this;
        }

        public Builder cleanConnection(boolean cleanConnection) {
            this.options.cleanConnection = cleanConnection;
            return this;
        }

        public Builder willTopic(String willTopic) {
            this.options.willTopic = willTopic;
            return this;
        }

        public Builder willPayload(String willPayload) {
            this.options.willPayload = willPayload;
            return this;
        }

        public Builder willRetain(boolean retain){
            this.options.willRetain = retain;
            return this;
        }

        public Builder willQos(int qos){
            this.options.setWillQos(qos);
            return this;
        }

        public Builder reconnectDelay(long reconnectDelay) {
            this.options.reconnectDelay = reconnectDelay;
            return this;
        }

        public Builder reconnectDelayMax(long reconnectDelayMax) {
            this.options.reconnectDelayMax = reconnectDelayMax;
            return this;
        }

        public Builder reconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
            this.options.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
            return this;
        }

        public Builder reconnectAttemptsMax(long reconnectAttemptsMax) {
            this.options.reconnectAttemptsMax = reconnectAttemptsMax;
            return this;
        }

        public Builder connectAttemptsMax(long connectAttemptsMax) {
            this.options.connectAttemptsMax = connectAttemptsMax;
            return this;
        }

        public Builder userName(String userName) {
            this.options.userName = userName;
            return this;
        }

        public Builder password(String password) {
            this.options.password = password;
            return this;
        }

        public Builder qos(int qos){
            this.options.setQos(qos);
            return this;
        }

        public MqttOptions build() {
            return this.options;
        }
    }
}
