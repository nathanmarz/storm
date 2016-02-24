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

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class Rebalance {

    private static final Logger LOG = LoggerFactory.getLogger(Rebalance.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("w", "wait", null, CLI.AS_INT)
            .opt("n", "num-workers", null, CLI.AS_INT)
            .opt("e", "executor", null, new ExecutorParser(), CLI.INTO_MAP)
            .arg("topologyName", CLI.FIRST_WINS)
            .parse(args);
        final String name = (String) cl.get("topologyName");
        final RebalanceOptions rebalanceOptions = new RebalanceOptions();
        Integer wait = (Integer) cl.get("w");
        Integer numWorkers = (Integer) cl.get("n");
        Map<String, Integer> numExecutors = (Map<String, Integer>) cl.get("e");

        if (null != wait) {
            rebalanceOptions.set_wait_secs(wait);
        }
        if (null != numWorkers) {
            rebalanceOptions.set_num_workers(numWorkers);
        }
        if (null != numExecutors) {
            rebalanceOptions.set_num_executors(numExecutors);
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Client nimbus) throws Exception {
                nimbus.rebalance(name, rebalanceOptions);
                LOG.info("Topology {} is rebalancing", name);
            }
        });
    }


    static final class ExecutorParser implements CLI.Parse {

        @Override
        public Object parse(String value) {
            try {
                int splitIndex = value.lastIndexOf('=');
                String componentName = value.substring(0, splitIndex);
                Integer parallelism = Integer.parseInt(value.substring(splitIndex + 1));
                Map<String, Integer> result = new HashMap<String, Integer>();
                result.put(componentName, parallelism);
                return result;
            } catch (Throwable ex) {
                throw new IllegalArgumentException(
                    format("Failed to parse '%s' correctly. Expected in <component>=<parallelism> format", value), ex);
            }
        }
    }

}
