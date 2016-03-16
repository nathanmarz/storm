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

package org.apache.storm.command;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.Level;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SetLogLevel {

    private static final Logger LOG = LoggerFactory.getLogger(SetLogLevel.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("l", "log-setting", null, new LogLevelsParser(LogLevelAction.UPDATE), CLI.INTO_MAP)
            .opt("r", "remove-log-setting", null, new LogLevelsParser(LogLevelAction.REMOVE), CLI.INTO_MAP)
            .arg("topologyName", CLI.FIRST_WINS)
            .parse(args);
        final String topologyName = (String) cl.get("topologyName");
        final LogConfig logConfig = new LogConfig();
        Map<String, LogLevel> logLevelMap = new HashMap<>();
        Map<String, LogLevel> updateLogLevel = (Map<String, LogLevel>) cl.get("l");
        if (null != updateLogLevel) {
            logLevelMap.putAll(updateLogLevel);
        }
        Map<String, LogLevel> removeLogLevel = (Map<String, LogLevel>) cl.get("r");
        if (null != removeLogLevel) {
            logLevelMap.putAll(removeLogLevel);
        }

        for (Map.Entry<String, LogLevel> entry : logLevelMap.entrySet()) {
            logConfig.put_to_named_logger_level(entry.getKey(), entry.getValue());
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Client nimbus) throws Exception {
                String topologyId = Utils.getTopologyId(topologyName, nimbus);
                if (null == topologyId) {
                    throw new IllegalArgumentException(topologyName + " is not a running topology");
                }
                nimbus.setLogConfig(topologyId, logConfig);
                LOG.info("Log config {} is sent for topology {}", logConfig, topologyName);
            }
        });
    }

    /**
     * Parses [logger name]=[level string]:[optional timeout],[logger name2]...
     *
     * e.g. ROOT=DEBUG:30
     *     root logger, debug for 30 seconds
     *
     *     org.apache.foo=WARN
     *     org.apache.foo set to WARN indefinitely
     */
    static final class LogLevelsParser implements CLI.Parse {

        private LogLevelAction action;

        public LogLevelsParser(LogLevelAction action) {
            this.action = action;
        }

        @Override
        public Object parse(String value) {
            final LogLevel logLevel = new LogLevel();
            logLevel.set_action(action);
            String name = null;
            if (action == LogLevelAction.REMOVE) {
                name = value;
            } else {
                String[] splits = value.split("=");
                Preconditions.checkArgument(splits.length == 2, "Invalid log string '%s'", value);
                name = splits[0];
                splits = splits[1].split(":");
                Integer timeout = 0;
                Level level = Level.valueOf(splits[0]);
                logLevel.set_reset_log_level(level.toString());
                if (splits.length > 1) {
                    timeout = Integer.parseInt(splits[1]);
                }
                logLevel.set_reset_log_level_timeout_secs(timeout);
            }
            Map<String, LogLevel> result = new HashMap<>();
            result.put(name, logLevel);
            return result;
        }
    }
}
