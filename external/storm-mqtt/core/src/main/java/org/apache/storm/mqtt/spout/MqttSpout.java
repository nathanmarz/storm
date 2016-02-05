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

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.mqtt.common.MqttUtils;
import org.apache.storm.mqtt.common.SslUtils;
import org.apache.storm.mqtt.ssl.KeyStoreLoader;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttSpout implements IRichSpout, Listener {
    private static final Logger LOG = LoggerFactory.getLogger(MqttSpout.class);

    private String topologyName;


    private CallbackConnection connection;

    protected transient SpoutOutputCollector collector;
    protected transient TopologyContext context;
    protected transient LinkedBlockingQueue<AckableMessage> incoming;
    protected transient HashMap<Long, AckableMessage> pending;
    private transient Map conf;
    protected MqttMessageMapper type;
    protected MqttOptions options;
    protected KeyStoreLoader keyStoreLoader;

    private boolean mqttConnected = false;
    private boolean mqttConnectFailed = false;


    private Long sequence = Long.MIN_VALUE;

    private Long nextId(){
        this.sequence++;
        if(this.sequence == Long.MAX_VALUE){
            this.sequence = Long.MIN_VALUE;
        }
        return this.sequence;
    }

    protected MqttSpout(){}

    public MqttSpout(MqttMessageMapper type, MqttOptions options){
        this(type, options, null);
    }

    public MqttSpout(MqttMessageMapper type, MqttOptions options, KeyStoreLoader keyStoreLoader){
        this.type = type;
        this.options = options;
        this.keyStoreLoader = keyStoreLoader;
        SslUtils.checkSslConfig(this.options.getUrl(), this.keyStoreLoader);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(this.type.outputFields());
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.topologyName = (String)conf.get(Config.TOPOLOGY_NAME);

        this.collector = collector;
        this.context = context;
        this.conf = conf;

        this.incoming = new LinkedBlockingQueue<>();
        this.pending = new HashMap<>();

        try {
            connectMqtt();
        } catch (Exception e) {
            this.collector.reportError(e);
            throw new RuntimeException("MQTT Connection failed.", e);
        }

    }

    private void connectMqtt() throws Exception {
        String clientId = this.topologyName + "-" + this.context.getThisComponentId() + "-" +
                this.context.getThisTaskId();

        MQTT client = MqttUtils.configureClient(this.options, clientId, this.keyStoreLoader);
        this.connection = client.callbackConnection();
        this.connection.listener(this);
        this.connection.connect(new ConnectCallback());

        while(!this.mqttConnected && !this.mqttConnectFailed){
            LOG.info("Waiting for connection...");
            Thread.sleep(500);
        }

        if(this.mqttConnected){
            List<String> topicList = this.options.getTopics();
            Topic[] topics = new Topic[topicList.size()];
            QoS qos = MqttUtils.qosFromInt(this.options.getQos());
            for(int i = 0;i < topicList.size();i++){
                topics[i] = new Topic(topicList.get(i), qos);
            }
            connection.subscribe(topics, new SubscribeCallback());
        }
    }



    public void close() {
        this.connection.disconnect(new DisconnectCallback());
    }

    public void activate() {
    }

    public void deactivate() {
    }

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    public void nextTuple() {
        AckableMessage tm = this.incoming.poll();
        if(tm != null){
            Long id = nextId();
            this.collector.emit(this.type.toValues(tm.getMessage()), id);
            this.pending.put(id, tm);
        } else {
            Thread.yield();
        }

    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier
     * has been fully processed. Typically, an implementation of this method will take that
     * message off the queue and prevent it from being replayed.
     *
     * @param msgId
     */
    public void ack(Object msgId) {
        AckableMessage msg = this.pending.remove(msgId);
        this.connection.getDispatchQueue().execute(msg.ack());
    }

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to be
     * fully processed. Typically, an implementation of this method will put that
     * message back on the queue to be replayed at a later time.
     *
     * @param msgId
     */
    public void fail(Object msgId) {
        try {
            this.incoming.put(this.pending.remove(msgId));
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while re-queueing message.", e);
        }
    }


    // ################# Listener Implementation ######################
    public void onConnected() {
        // this gets called repeatedly for no apparent reason, don't do anything
    }

    public void onDisconnected() {
        // this gets called repeatedly for no apparent reason, don't do anything
    }

    public void onPublish(UTF8Buffer topic, Buffer payload, Runnable ack) {
        LOG.debug("Received message: topic={}, payload={}", topic.toString(), new String(payload.toByteArray()));
        try {
            this.incoming.put(new AckableMessage(topic.toString(), payload.toByteArray(), ack));
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while queueing an MQTT message.");
        }
    }

    public void onFailure(Throwable throwable) {
        LOG.error("MQTT Connection Failure.", throwable);
        MqttSpout.this.connection.disconnect(new DisconnectCallback());
        throw new RuntimeException("MQTT Connection failure.", throwable);
    }

    // ################# Connect Callback Implementation ######################
    private class ConnectCallback implements Callback<Void> {
        public void onSuccess(Void v) {
            LOG.info("MQTT Connected. Subscribing to topic...");
            MqttSpout.this.mqttConnected = true;
        }

        public void onFailure(Throwable throwable) {
            LOG.info("MQTT Connection failed.");
            MqttSpout.this.mqttConnectFailed = true;
        }
    }

    // ################# Subscribe Callback Implementation ######################
    private class SubscribeCallback implements Callback<byte[]>{
        public void onSuccess(byte[] qos) {
            LOG.info("Subscripton sucessful.");
        }

        public void onFailure(Throwable throwable) {
            LOG.error("MQTT Subscripton failed.", throwable);
            throw new RuntimeException("MQTT Subscribe failed.", throwable);
        }
    }

    // ################# Subscribe Callback Implementation ######################
    private class DisconnectCallback implements Callback<Void>{
        public void onSuccess(Void aVoid) {
            LOG.info("MQTT Disconnect successful.");
        }

        public void onFailure(Throwable throwable) {
            // Disconnects don't fail.
        }
    }

}
