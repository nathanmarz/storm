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

import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBPulse;
import org.apache.storm.generated.HBNodes;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;


import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Pacemaker implements IServerMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Pacemaker.class);

    private Map<String, byte[]> heartbeats;
    private PacemakerStats lastOneMinStats;
    private PacemakerStats pacemakerStats;
    private Map conf;
    private final long sleepSeconds = 60;

    private boolean isDaemon = true;
    private boolean startImmediately = true;

    private static class PacemakerStats {
        public AtomicInteger sendPulseCount = new AtomicInteger();
        public AtomicInteger totalReceivedSize = new AtomicInteger();
        public AtomicInteger getPulseCount = new AtomicInteger();
        public AtomicInteger totalSentSize = new AtomicInteger();
        public AtomicInteger largestHeartbeatSize = new AtomicInteger();
        public AtomicInteger averageHeartbeatSize = new AtomicInteger();
        private AtomicInteger totalKeys = new AtomicInteger();
    }
    private static class PaceMakerDynamicMBean implements DynamicMBean{

        private final MBeanInfo mBeanInfo;
        private final static String [] attributeNames = new String []{
                "send-pulse-count",
                "total-received-size",
                "get-pulse-count",
                "total-sent-size",
                "largest-heartbeat-size",
                "average-heartbeat-size",
                "total-keys"
        };
        private static String attributeType = "java.util.concurrent.atomic.AtomicInteger";

        private static final MBeanAttributeInfo[] attributeInfos = new MBeanAttributeInfo[] { 
                        new MBeanAttributeInfo("send-pulse-count", attributeType, "send-pulse-count", true, false, false),
                        new MBeanAttributeInfo("total-received-size", attributeType, "total-received-size", true, false, false),
                        new MBeanAttributeInfo("get-pulse-count", attributeType, "get-pulse-count", true, false, false),
                        new MBeanAttributeInfo("total-sent-size", attributeType, "total-sent-size", true, false, false),
                        new MBeanAttributeInfo("largest-heartbeat-size", attributeType, "largest-heartbeat-size", true, false, false),
                        new MBeanAttributeInfo("average-heartbeat-size", attributeType, "average-heartbeat-size", true, false, false),
                        new MBeanAttributeInfo("total-keys", attributeType, "total-keys", true, false, false)
        };
        private PacemakerStats stats;

        public PaceMakerDynamicMBean(PacemakerStats stats) {
            this.stats = stats;
            this.mBeanInfo = new MBeanInfo("org.apache.storm.pacemaker.PaceMakerDynamicMBean", "Java Pacemaker Dynamic MBean",
                    PaceMakerDynamicMBean.attributeInfos, null, null, null);
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            return mBeanInfo;
        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            AttributeList list = new AttributeList();
            if (attributes == null)
                return list;
            final int len = attributes.length;
            try {
                for (int i = 0; i < len; i++) {
                    final Attribute a = new Attribute(attributes[i], getAttribute(attributes[i]));
                    list.add(a);

                }
            } catch (Exception e) {
                throw Utils.wrapInRuntime(e);
            }
            return list;
        }

        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
            if (attribute == null)
                throw new AttributeNotFoundException("null");
            if (attribute.equals("send-pulse-count"))
                return stats.sendPulseCount.get();
            else if (attribute.equals("total-received-size"))
                return stats.totalReceivedSize.get();
            else if (attribute.equals("get-pulse-count"))
                return stats.getPulseCount.get();
            else if (attribute.equals("total-sent-size"))
                return stats.totalSentSize.get();
            else if (attribute.equals("largest-heartbeat-size"))
                return stats.largestHeartbeatSize.get();
            else if (attribute.equals("average-heartbeat-size"))
                return stats.averageHeartbeatSize.get();
            else if (attribute.equals("total-keys"))
                return stats.totalKeys.get();
            else
                throw new AttributeNotFoundException("null");
        }

        @Override
        public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            return null;
        }

        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
            return null;
        }
    }

    public Pacemaker(Map conf) {
        heartbeats = new ConcurrentHashMap();
        pacemakerStats = new PacemakerStats();
        lastOneMinStats = new PacemakerStats();
        this.conf = conf;
        startStatsThread();
        registerJmx(lastOneMinStats);
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

    private void registerJmx (PacemakerStats lastOneMinStats){
        try {
            MBeanServer mbServer = ManagementFactory.getPlatformMBeanServer();
            DynamicMBean dynamicMBean = new PaceMakerDynamicMBean(lastOneMinStats);
            ObjectName objectname = new ObjectName("org.apache.storm.pacemaker.Pacemaker:stats=lastOneMinStats");
            mbServer.registerMBean(dynamicMBean, objectname);
        }catch (Exception e){
            throw Utils.wrapInRuntime(e);
        }
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
        pacemakerStats.sendPulseCount.incrementAndGet();
        pacemakerStats.totalReceivedSize.addAndGet(details.length);
        updateLargestHbSize(details.length);
        updateAverageHbSize(details.length);
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
            pacemakerStats.getPulseCount.incrementAndGet();
            if (details != null) {
                pacemakerStats.totalSentSize.addAndGet(details.length);
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

    private void updateLargestHbSize(int size) {
        int newValue = size;
        while (true) {
            int oldValue = pacemakerStats.largestHeartbeatSize.get();
            if (newValue > oldValue) {
                if (!pacemakerStats.largestHeartbeatSize.compareAndSet(oldValue, newValue))
                    continue;
            }
            break;
        }
    }

    private void updateAverageHbSize(int size) {
        while (true) {
            int oldValue = pacemakerStats.averageHeartbeatSize.get();
            int count = pacemakerStats.sendPulseCount.get();
            int newValue = ((count * oldValue) + size) / (count + 1);
            if (pacemakerStats.averageHeartbeatSize.compareAndSet(oldValue, newValue))
                break;
        }
    }

    private void startStatsThread() {
        Callable afn = new Callable() {
            public Object call() {
                int sendCount = pacemakerStats.sendPulseCount.getAndSet(0);
                int receivedSize = pacemakerStats.totalReceivedSize.getAndSet(0);
                int getCount = pacemakerStats.getPulseCount.getAndSet(0);
                int sentSize = pacemakerStats.totalSentSize.getAndSet(0);
                int largest = pacemakerStats.largestHeartbeatSize.getAndSet(0);
                int average = pacemakerStats.averageHeartbeatSize.getAndSet(0);
                int totalKeys = heartbeats.size();
                LOG.debug("\nReceived {} heartbeats totaling {} bytes,\nSent {} heartbeats totaling {} bytes," +
                          "\nThe largest heartbeat was {} bytes,\nThe average heartbeat was {} bytes,\n" +
                          "Pacemaker contained {} total keys\nin the last {} second(s)",
                        sendCount, receivedSize, getCount, sentSize, largest, average, totalKeys, sleepSeconds);
                lastOneMinStats.sendPulseCount.set(sendCount);
                lastOneMinStats.totalReceivedSize.set(receivedSize);
                lastOneMinStats.getPulseCount.set(getCount);
                lastOneMinStats.totalSentSize.set(sentSize);
                lastOneMinStats.largestHeartbeatSize.set(largest);
                lastOneMinStats.averageHeartbeatSize.set(average);
                lastOneMinStats.averageHeartbeatSize.set(totalKeys);
                return sleepSeconds; // Run only once.
            }
        };
        Utils.asyncLoop(afn, isDaemon, null, Thread.currentThread().getPriority(), false, startImmediately, null);
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
