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
package org.apache.storm.daemon;

import com.codahale.metrics.MetricRegistry;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.Thrift;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.metric.EventLoggerBolt;
import org.apache.storm.metric.MetricsConsumerBolt;
import org.apache.storm.metric.SystemBolt;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.task.IBolt;
import org.apache.storm.testing.NonRichBoltTracker;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class StormCommon {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static StormCommon _instance = new StormCommon();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param common a StormCommon instance
     * @return the previously set instance
     */
    public static StormCommon setInstance(StormCommon common) {
        StormCommon oldInstance = _instance;
        _instance = common;
        return oldInstance;
    }

    private static final Logger LOG = LoggerFactory.getLogger(StormCommon.class);

    public static final String SYSTEM_STREAM_ID = "__system";

    public static final String EVENTLOGGER_COMPONENT_ID = "__eventlogger";
    public static final String EVENTLOGGER_STREAM_ID = "__eventlog";

    public static String getStormId(final IStormClusterState stormClusterState, final String topologyName) {
        List<String> activeTopologys = stormClusterState.activeStorms();
        IPredicate pred = new IPredicate<String>() {
            @Override
            public boolean test(String obj) {
                String name = stormClusterState.stormBase(obj, null).get_name();
                return name.equals(topologyName);
            }
        };
        return Utils.findOne(pred, activeTopologys);
    }

    public static Map<String, StormBase> topologyBases(IStormClusterState stormClusterState) {
        return _instance.topologyBasesImpl(stormClusterState);
    }

    protected Map<String, StormBase> topologyBasesImpl(IStormClusterState stormClusterState) {
        List<String> activeTopologys = stormClusterState.activeStorms();
        Map<String, StormBase> stormBases = new HashMap<String, StormBase>();
        for (String topologyId : activeTopologys) {
            StormBase base = stormClusterState.stormBase(topologyId, null);
            stormBases.put(topologyId, base);
        }
        return stormBases;
    }

    public static void validateDistributedMode(Map conf) {
        if (ConfigUtils.isLocalMode(conf)) {
            throw new IllegalArgumentException("Cannot start server in local mode!");
        }
    }

    private static void validateIds(StormTopology topology) throws InvalidTopologyException {
        List<String> componentIds = new ArrayList<String>();

        for (StormTopology._Fields field : Thrift.getTopologyFields()) {
            if (ThriftTopologyUtils.isWorkerHook(field) == false) {
                Object value = topology.getFieldValue(field);
                Map<String, Object> componentMap = (Map<String, Object>) value;
                componentIds.addAll(componentMap.keySet());

                for (String id : componentMap.keySet()) {
                    if (Utils.isSystemId(id)) {
                        throw new InvalidTopologyException(id + " is not a valid component id.");
                    }
                }
                for (Object componentObj : componentMap.values()) {
                    ComponentCommon common = getComponentCommon(componentObj);
                    Set<String> streamIds = common.get_streams().keySet();
                    for (String id : streamIds) {
                        if (Utils.isSystemId(id)) {
                            throw new InvalidTopologyException(id + " is not a valid stream id.");
                        }
                    }
                }
            }
        }

        List<String> offending = Utils.getRepeat(componentIds);
        if (offending.isEmpty() == false) {
            throw new InvalidTopologyException("Duplicate component ids: " + offending);
        }
    }

    private static boolean isEmptyInputs(ComponentCommon common) {
        if (common.get_inputs() == null) {
            return true;
        } else {
            return common.get_inputs().isEmpty();
        }
    }

    public static Map<String, Object> allComponents(StormTopology topology) {
        Map<String, Object> components = new HashMap<String, Object>();
        List<StormTopology._Fields> topologyFields = Arrays.asList(Thrift.getTopologyFields());
        for (StormTopology._Fields field : topologyFields) {
            if (ThriftTopologyUtils.isWorkerHook(field) == false) {
                components.putAll(((Map) topology.getFieldValue(field)));
            }
        }
        return components;
    }

    public static Map componentConf(Object component) {
        Map<Object, Object> conf = new HashMap<Object, Object>();
        ComponentCommon common = getComponentCommon(component);
        String jconf = common.get_json_conf();
        if (jconf != null) {
            conf.putAll((Map<Object, Object>) JSONValue.parse(jconf));
        }
        return conf;
    }

    public static void validateBasic(StormTopology topology) throws InvalidTopologyException {
        validateIds(topology);

        for (StormTopology._Fields field : Thrift.getSpoutFields()) {
            Map<String, Object> spoutComponents = (Map<String, Object>) topology.getFieldValue(field);
            if (spoutComponents != null) {
                for (Object obj : spoutComponents.values()) {
                    ComponentCommon common = getComponentCommon(obj);
                    if (isEmptyInputs(common) == false) {
                        throw new InvalidTopologyException("May not declare inputs for a spout");
                    }
                }
            }
        }

        Map<String, Object> componentMap = allComponents(topology);
        for (Object componentObj : componentMap.values()) {
            Map conf = componentConf(componentObj);
            ComponentCommon common = getComponentCommon(componentObj);
            int parallelismHintNum = Thrift.getParallelismHint(common);
            Integer taskNum = Utils.getInt(conf.get(Config.TOPOLOGY_TASKS), 0);
            if (taskNum > 0 && parallelismHintNum <= 0) {
                throw new InvalidTopologyException("Number of executors must be greater than 0 when number of tasks is greater than 0");
            }
        }
    }

    private static Set<String> getStreamOutputFields(Map<String, StreamInfo> streams) {
        Set<String> outputFields = new HashSet<String>();
        for (StreamInfo streamInfo : streams.values()) {
            outputFields.addAll(streamInfo.get_output_fields());
        }
        return outputFields;
    }

    public static void validateStructure(StormTopology topology) throws InvalidTopologyException {
        Map<String, Object> componentMap = allComponents(topology);
        for (Map.Entry<String, Object> entry : componentMap.entrySet()) {
            String componentId = entry.getKey();
            ComponentCommon common = getComponentCommon(entry.getValue());
            Map<GlobalStreamId, Grouping> inputs = common.get_inputs();
            for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
                String sourceStreamId = input.getKey().get_streamId();
                String sourceComponentId = input.getKey().get_componentId();
                if(componentMap.keySet().contains(sourceComponentId) == false) {
                    throw new InvalidTopologyException("Component: [" + componentId + "] subscribes from non-existent component [" + sourceComponentId + "]");
                }

                ComponentCommon sourceComponent = getComponentCommon(componentMap.get(sourceComponentId));
                if (sourceComponent.get_streams().containsKey(sourceStreamId) == false) {
                    throw new InvalidTopologyException("Component: [" + componentId + "] subscribes from non-existent stream: " +
                            "[" + sourceStreamId + "] of component [" + sourceComponentId + "]");
                }

                Grouping grouping = input.getValue();
                if (Thrift.groupingType(grouping) == Grouping._Fields.FIELDS) {
                    List<String> fields = new ArrayList<String>(grouping.get_fields());
                    Map<String, StreamInfo> streams = sourceComponent.get_streams();
                    Set<String> sourceOutputFields = getStreamOutputFields(streams);
                    fields.removeAll(sourceOutputFields);
                    if (fields.size() != 0) {
                        throw new InvalidTopologyException("Component: [" + componentId + "] subscribes from stream: [" + sourceStreamId  +"] of component " +
                                "[" + sourceComponentId + "] + with non-existent fields: " + fields);
                    }
                }
            }
        }
    }

    public static Map<GlobalStreamId, Grouping> ackerInputs(StormTopology topology) {
        Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId, Grouping>();
        Set<String> boltIds = topology.get_bolts().keySet();
        Set<String> spoutIds = topology.get_spouts().keySet();

        for(String id : spoutIds) {
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_INIT_STREAM_ID), Thrift.prepareFieldsGrouping(Arrays.asList("id")));
        }

        for(String id : boltIds) {
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_ACK_STREAM_ID), Thrift.prepareFieldsGrouping(Arrays.asList("id")));
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_FAIL_STREAM_ID), Thrift.prepareFieldsGrouping(Arrays.asList("id")));
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_RESET_TIMEOUT_STREAM_ID), Thrift.prepareFieldsGrouping(Arrays.asList("id")));
        }
        return inputs;
    }

    public static IBolt makeAckerBolt() {
        return _instance.makeAckerBoltImpl();
    }
    public IBolt makeAckerBoltImpl() {
        return new Acker();
    }

    public static void addAcker(Map conf, StormTopology topology) {
        int ackerNum = Utils.getInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), Utils.getInt(conf.get(Config.TOPOLOGY_WORKERS)));
        Map<GlobalStreamId, Grouping> inputs = ackerInputs(topology);

        Map<String, StreamInfo> outputStreams = new HashMap<String, StreamInfo>();
        outputStreams.put(Acker.ACKER_ACK_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id")));
        outputStreams.put(Acker.ACKER_FAIL_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id")));
        outputStreams.put(Acker.ACKER_RESET_TIMEOUT_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id")));

        Map<String, Object> ackerConf = new HashMap<String, Object>();
        ackerConf.put(Config.TOPOLOGY_TASKS, ackerNum);
        ackerConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));

        Bolt acker = Thrift.prepareSerializedBoltDetails(inputs, makeAckerBolt(), outputStreams, ackerNum, ackerConf);

        for(Bolt bolt : topology.get_bolts().values()) {
            ComponentCommon common = bolt.get_common();
            common.put_to_streams(Acker.ACKER_ACK_STREAM_ID, Thrift.outputFields(Arrays.asList("id", "ack-val")));
            common.put_to_streams(Acker.ACKER_FAIL_STREAM_ID, Thrift.outputFields(Arrays.asList("id")));
            common.put_to_streams(Acker.ACKER_RESET_TIMEOUT_STREAM_ID, Thrift.outputFields(Arrays.asList("id")));
        }

        for (SpoutSpec spout : topology.get_spouts().values()) {
            ComponentCommon common = spout.get_common();
            Map spoutConf = componentConf(spout);
            spoutConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
            common.set_json_conf(JSONValue.toJSONString(spoutConf));
            common.put_to_streams(Acker.ACKER_INIT_STREAM_ID, Thrift.outputFields(Arrays.asList("id", "init-val", "spout-task")));
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_ACK_STREAM_ID), Thrift.prepareDirectGrouping());
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_FAIL_STREAM_ID), Thrift.prepareDirectGrouping());
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_RESET_TIMEOUT_STREAM_ID), Thrift.prepareDirectGrouping());
        }

        topology.put_to_bolts(Acker.ACKER_COMPONENT_ID, acker);
    }

    public static ComponentCommon getComponentCommon(Object component) {
        ComponentCommon common = null;
        if (component instanceof StateSpoutSpec) {
            common = ((StateSpoutSpec) component).get_common();
        } else if (component instanceof SpoutSpec) {
            common = ((SpoutSpec) component).get_common();
        } else if (component instanceof Bolt) {
            common = ((Bolt) component).get_common();
        }
        return common;
    }

    public static void addMetricStreams(StormTopology topology) {
        for (Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            StreamInfo streamInfo = Thrift.outputFields(Arrays.asList("task-info", "data-points"));
            common.put_to_streams(Constants.METRICS_STREAM_ID, streamInfo);
        }
    }

    public static void addSystemStreams(StormTopology topology) {
        for (Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            StreamInfo streamInfo = Thrift.outputFields(Arrays.asList("event"));
            common.put_to_streams(SYSTEM_STREAM_ID, streamInfo);
        }
    }

    public static List<String> eventLoggerBoltFields() {
        List<String> fields = Arrays.asList(EventLoggerBolt.FIELD_COMPONENT_ID, EventLoggerBolt.FIELD_MESSAGE_ID, EventLoggerBolt.FIELD_TS,
                EventLoggerBolt.FIELD_VALUES);
        return fields;
    }

    public static Map<GlobalStreamId, Grouping> eventLoggerInputs(StormTopology topology) {
        Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId, Grouping>();
        Set<String> allIds = new HashSet<String>();
        allIds.addAll(topology.get_bolts().keySet());
        allIds.addAll(topology.get_spouts().keySet());

        for(String id : allIds) {
            inputs.put(Utils.getGlobalStreamId(id, EVENTLOGGER_STREAM_ID), Thrift.prepareFieldsGrouping(Arrays.asList("component-id")));
        }
        return inputs;
    }

    public static void addEventLogger(Map conf, StormTopology topology) {
        Integer numExecutors = Utils.getInt(conf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS), Utils.getInt(conf.get(Config.TOPOLOGY_WORKERS)));
        HashMap<String, Object> componentConf = new HashMap<String, Object>();
        componentConf.put(Config.TOPOLOGY_TASKS, numExecutors);
        componentConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
        Bolt eventLoggerBolt = Thrift.prepareSerializedBoltDetails(eventLoggerInputs(topology), new EventLoggerBolt(), null, numExecutors, componentConf);

        for(Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            common.put_to_streams(EVENTLOGGER_STREAM_ID, Thrift.outputFields(eventLoggerBoltFields()));
        }
        topology.put_to_bolts(EVENTLOGGER_COMPONENT_ID, eventLoggerBolt);
    }

    public static Map<String, Bolt> metricsConsumerBoltSpecs(Map conf, StormTopology topology) {
        Map<String, Bolt> metricsConsumerBolts = new HashMap<String, Bolt>();

        Set<String> componentIdsEmitMetrics = new HashSet<String>();
        componentIdsEmitMetrics.addAll(allComponents(topology).keySet());
        componentIdsEmitMetrics.add(Constants.SYSTEM_COMPONENT_ID);

        Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId, Grouping>();
        for (String componentId : componentIdsEmitMetrics) {
            inputs.put(Utils.getGlobalStreamId(componentId, Constants.METRICS_STREAM_ID), Thrift.prepareShuffleGrouping());
        }

        List<Map<String, Object>> registerInfo = (List<Map<String, Object>>) conf.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if (registerInfo != null) {
            Map<String, Integer> classOccurrencesMap = new HashMap<String, Integer>();
            for (Map<String, Object> info : registerInfo) {
                String className = (String) info.get("class");
                Object argument = info.get("argument");
                Integer phintNum = Utils.getInt(info.get("parallelism.hint"), 1);
                Map<String, Object> metricsConsumerConf = new HashMap<String, Object>();
                metricsConsumerConf.put(Config.TOPOLOGY_TASKS, phintNum);
                Bolt metricsConsumerBolt = Thrift.prepareSerializedBoltDetails(inputs, new MetricsConsumerBolt(className, argument), null, phintNum, metricsConsumerConf);

                String id = className;
                if (classOccurrencesMap.containsKey(className)) {
                    // e.g. [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]"
                    int occurrenceNum = classOccurrencesMap.get(className);
                    occurrenceNum++;
                    classOccurrencesMap.put(className, occurrenceNum);
                    id = Constants.METRICS_COMPONENT_ID_PREFIX + className + "#" + occurrenceNum;
                } else {
                    classOccurrencesMap.put(className, 1);
                }
                metricsConsumerBolts.put(id, metricsConsumerBolt);
            }
        }
        return metricsConsumerBolts;
    }

    public static void addMetricComponents(Map conf, StormTopology topology) {
        Map<String, Bolt> metricsConsumerBolts = metricsConsumerBoltSpecs(conf, topology);
        for (Map.Entry<String, Bolt> entry : metricsConsumerBolts.entrySet()) {
            topology.put_to_bolts(entry.getKey(), entry.getValue());
        }
    }

    public static void addSystemComponents(Map conf, StormTopology topology) {
        Map<String, StreamInfo> outputStreams = new HashMap<String, StreamInfo>();
        outputStreams.put(Constants.SYSTEM_TICK_STREAM_ID, Thrift.outputFields(Arrays.asList("rate_secs")));
        outputStreams.put(Constants.METRICS_TICK_STREAM_ID, Thrift.outputFields(Arrays.asList("interval")));
        outputStreams.put(Constants.CREDENTIALS_CHANGED_STREAM_ID, Thrift.outputFields(Arrays.asList("creds")));

        Map<String, Object> boltConf = new HashMap<String, Object>();
        boltConf.put(Config.TOPOLOGY_TASKS, 0);

        Bolt systemBoltSpec = Thrift.prepareSerializedBoltDetails(null, new SystemBolt(), outputStreams, 0, boltConf);
        topology.put_to_bolts(Constants.SYSTEM_COMPONENT_ID, systemBoltSpec);
    }

    public static StormTopology systemTopology(Map stormConf, StormTopology topology) throws InvalidTopologyException {
        return _instance.systemTopologyImpl(stormConf, topology);
    }

    protected StormTopology systemTopologyImpl(Map stormConf, StormTopology topology) throws InvalidTopologyException {
        validateBasic(topology);

        StormTopology ret = topology.deepCopy();
        addAcker(stormConf, ret);
        addEventLogger(stormConf, ret);
        addMetricComponents(stormConf, ret);
        addSystemComponents(stormConf, ret);
        addMetricStreams(ret);
        addSystemStreams(ret);

        validateStructure(ret);

        return ret;
    }

    public static boolean hasAckers(Map stormConf) {
        Object ackerNum = stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS);
        if (ackerNum == null || Utils.getInt(ackerNum) > 0) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean hasEventLoggers(Map stormConf) {
        Object eventLoggerNum = stormConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS);
        if (eventLoggerNum == null || Utils.getInt(eventLoggerNum) > 0) {
            return true;
        } else {
            return false;
        }
    }

    public static int numStartExecutors(Object component) throws InvalidTopologyException {
        ComponentCommon common = getComponentCommon(component);
        return Thrift.getParallelismHint(common);
    }

    public static Map<Integer, String> stormTaskInfo(StormTopology userTopology, Map stormConf) throws InvalidTopologyException {
        return _instance.stormTaskInfoImpl(userTopology, stormConf);
    }
    /*
     * Returns map from task -> componentId
     */
    protected Map<Integer, String> stormTaskInfoImpl(StormTopology userTopology, Map stormConf) throws InvalidTopologyException {
        Map<Integer, String> taskIdToComponentId = new HashMap<Integer, String>();

        StormTopology systemTopology = systemTopology(stormConf, userTopology);
        Map<String, Object> components = allComponents(systemTopology);
        Map<String, Integer> componentIdToTaskNum = new TreeMap<String, Integer>();
        for (Map.Entry<String, Object> entry : components.entrySet()) {
            Map conf = componentConf(entry.getValue());
            Object taskNum = conf.get(Config.TOPOLOGY_TASKS);
            componentIdToTaskNum.put(entry.getKey(), Utils.getInt(taskNum));
        }

        int taskId = 1;
        for (Map.Entry<String, Integer> entry : componentIdToTaskNum.entrySet()) {
            String componentId = entry.getKey();
            Integer taskNum = entry.getValue();
            while (taskNum > 0) {
                taskIdToComponentId.put(taskId, componentId);
                taskNum--;
                taskId++;
            }
        }
        return taskIdToComponentId;
    }

    public static List<Integer> executorIdToTasks(List<Long> executorId) {
        List<Integer> taskIds = new ArrayList<Integer>();
        int taskId = executorId.get(0).intValue();
        while (taskId <= executorId.get(1).intValue()) {
            taskIds.add(taskId);
            taskId++;
        }
        return taskIds;
    }

    public static Map<Integer, NodeInfo> taskToNodeport(Map<List<Long>, NodeInfo> executorToNodeport) {
        Map<Integer, NodeInfo> tasksToNodeport = new HashMap<Integer, NodeInfo>();
        for (Map.Entry<List<Long>, NodeInfo> entry : executorToNodeport.entrySet()) {
            List<Integer> taskIds = executorIdToTasks(entry.getKey());
            for (Integer taskId : taskIds) {
                tasksToNodeport.put(taskId, entry.getValue());
            }
        }
        return tasksToNodeport;
    }

    public static IAuthorizer mkAuthorizationHandler(String klassName, Map conf) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return _instance.mkAuthorizationHandlerImpl(klassName, conf);
    }

    protected IAuthorizer mkAuthorizationHandlerImpl(String klassName, Map conf) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        IAuthorizer aznHandler = null;
        if (klassName != null) {
            Class aznClass = Class.forName(klassName);
            if (aznClass != null) {
                aznHandler = (IAuthorizer) aznClass.newInstance();
                if (aznHandler != null) {
                    aznHandler.prepare(conf);
                }
                LOG.debug("authorization class name:{}, class:{}, handler:{}",klassName, aznClass, aznHandler);
            }
        }

        return aznHandler;
    }
}
