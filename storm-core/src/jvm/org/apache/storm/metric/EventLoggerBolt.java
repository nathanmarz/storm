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
package org.apache.storm.metric;

import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import static org.apache.storm.metric.IEventLogger.EventInfo;

public class EventLoggerBolt implements IBolt {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoggerBolt.class);

    /*
     The below field declarations are also used in common.clj to define the event logger output fields
      */
    public static final String FIELD_TS = "ts";
    public static final String FIELD_VALUES = "values";
    public static final String FIELD_COMPONENT_ID = "component-id";
    public static final String FIELD_MESSAGE_ID = "message-id";

    private IEventLogger eventLogger;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.info("EventLoggerBolt prepare called");
        eventLogger = new FileBasedEventLogger();
        eventLogger.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("** EventLoggerBolt got tuple from sourceComponent {}, with values {}", input.getSourceComponent(), input.getValues());

        Object msgId = input.getValueByField(FIELD_MESSAGE_ID);
        EventInfo eventInfo = new EventInfo(input.getValueByField(FIELD_TS).toString(), input.getSourceComponent(),
                                            String.valueOf(input.getSourceTask()), msgId == null ? "" : msgId.toString(),
                                            input.getValueByField(FIELD_VALUES).toString());

        eventLogger.log(eventInfo);
    }

    @Override
    public void cleanup() {
        eventLogger.close();
    }
}
