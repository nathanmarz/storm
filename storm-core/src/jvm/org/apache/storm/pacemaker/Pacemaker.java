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
package org.apache.storm.pacemaker;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBPulse;
import org.apache.storm.generated.HBNodes;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;


public class Pacemaker implements IServerMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Pacemaker.class);
    private Map<String, byte[]> heartbeats;
    private Map conf;

    private final static Meter meterSendPulseCount = StormMetricsRegistry.registerMeter("pacemaker:send-pulse-count");
    private final static Meter meterTotalReceivedSize = StormMetricsRegistry.registerMeter("pacemaker:total-receive-size");
    private final static Meter meterGetPulseCount = StormMetricsRegistry.registerMeter("pacemaker:get-pulse=count");
    private final static Meter meterTotalSentSize = StormMetricsRegistry.registerMeter("pacemaker:total-sent-size");
    private final static Histogram histogramHeartbeatSize = StormMetricsRegistry.registerHistogram("pacemaker:heartbeat-size", new ExponentiallyDecayingReservoir());


    public Pacemaker(Map conf) {
        heartbeats = new ConcurrentHashMap();
        this.conf = conf;
        StormMetricsRegistry.registerGauge("pacemaker:size-total-keys",
                new Callable() {
                    @Override
                    public Integer call() throws Exception {
                        return heartbeats.size();
                    }
                });
        StormMetricsRegistry.startMetricsReporters(conf);
    }

    @Override
    public HBMessage handleMessage(HBMessage m, boolean authenticated) {
        HBMessage response = null;
        HBMessageData data = m.get_data();
        switch (m.get_type()) {
        case CREATE_PATH:
            response = createPath(data.get_path());
            break;
        case EXISTS:
            response = pathExists(data.get_path(), authenticated);
            break;
        case SEND_PULSE:
            response = sendPulse(data.get_pulse());
            break;
        case GET_ALL_PULSE_FOR_PATH:
            response = getAllPulseForPath(data.get_path(), authenticated);
            break;
        case GET_ALL_NODES_FOR_PATH:
            response = getAllNodesForPath(data.get_path(), authenticated);
            break;
        case GET_PULSE:
            response = getPulse(data.get_path(), authenticated);
            break;
        case DELETE_PATH:
            response = deletePath(data.get_path());
            break;
        case DELETE_PULSE_ID:
            response = deletePulseId(data.get_path());
            break;
        default:
            LOG.info("Got Unexpected Type: {}", m.get_type());
            break;
        }
        if (response != null)
            response.set_message_id(m.get_message_id());
        return response;
    }


    private HBMessage createPath(String path) {
        return new HBMessage(HBServerMessageType.CREATE_PATH_RESPONSE, null);
    }

    private HBMessage pathExists(String path, boolean authenticated) {
        HBMessage response = null;
        if (authenticated) {
            boolean itDoes = heartbeats.containsKey(path);
            LOG.debug("Checking if path [ {} ] exists... {} .", path, itDoes);
            response = new HBMessage(HBServerMessageType.EXISTS_RESPONSE, HBMessageData.boolval(itDoes));
        } else {
            response = notAuthorized();
        }
        return response;
    }

    private HBMessage notAuthorized() {
        return new HBMessage(HBServerMessageType.NOT_AUTHORIZED, null);
    }

    private HBMessage sendPulse(HBPulse pulse) {
        String id = pulse.get_id();
        byte[] details = pulse.get_details();
        LOG.debug("Saving Pulse for id [ {} ] data [ {} ].", id, details);
        meterSendPulseCount.mark();
        meterTotalReceivedSize.mark(details.length);
        histogramHeartbeatSize.update(details.length);
        heartbeats.put(id, details);
        return new HBMessage(HBServerMessageType.SEND_PULSE_RESPONSE, null);
    }

    private HBMessage getAllPulseForPath(String path, boolean authenticated) {
        if (authenticated) {
            return new HBMessage(HBServerMessageType.GET_ALL_PULSE_FOR_PATH_RESPONSE, null);
        } else {
            return notAuthorized();
        }
    }

    private HBMessage getAllNodesForPath(String path, boolean authenticated) {
        LOG.debug("List all nodes for path {}", path);
        if (authenticated) {
            Set<String> pulseIds = new HashSet<>();
            for (String key : heartbeats.keySet()) {
                String[] replaceStr = key.replaceFirst(path, "").split("/");
                String trimmed = null;
                for (String str : replaceStr) {
                    if (!str.equals("")) {
                        trimmed = str;
                        break;
                    }
                }
                if (trimmed != null && key.indexOf(path) == 0) {
                    pulseIds.add(trimmed);
                }
            }
            HBMessageData hbMessageData = HBMessageData.nodes(new HBNodes(new ArrayList(pulseIds)));
            return new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, hbMessageData);
        } else {
            return notAuthorized();
        }
    }

    private HBMessage getPulse(String path, boolean authenticated) {
        if (authenticated) {
            byte[] details = heartbeats.get(path);
            LOG.debug("Getting Pulse for path [ {} ]...data [ {} ].", path, details);
            meterGetPulseCount.mark();
            if (details != null) {
                meterTotalSentSize.mark(details.length);
            }
            HBPulse hbPulse = new HBPulse();
            hbPulse.set_id(path);
            hbPulse.set_details(details);
            return new HBMessage(HBServerMessageType.GET_PULSE_RESPONSE, HBMessageData.pulse(hbPulse));
        } else {
            return notAuthorized();
        }
    }

    private HBMessage deletePath(String path) {
        String prefix = path.endsWith("/") ? path : (path + "/");
        for (String key : heartbeats.keySet()) {
            if (key.indexOf(prefix) == 0)
                deletePulseId(key);
        }
        return new HBMessage(HBServerMessageType.DELETE_PATH_RESPONSE, null);
    }

    private HBMessage deletePulseId(String path) {
        LOG.debug("Deleting Pulse for id [ {} ].", path);
        heartbeats.remove(path);
        return new HBMessage(HBServerMessageType.DELETE_PULSE_ID_RESPONSE, null);
    }

    private PacemakerServer launchServer() {
        LOG.info("Starting pacemaker server for storm version '{}", VersionInfo.getVersion());
        return new PacemakerServer(this, conf);
    }

    public static void main(String[] args) {
        SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
        Map conf = ConfigUtils.overrideLoginConfigWithSystemProperty(ConfigUtils.readStormConfig());
        final Pacemaker serverHandler = new Pacemaker(conf);
        serverHandler.launchServer();
    }

}
