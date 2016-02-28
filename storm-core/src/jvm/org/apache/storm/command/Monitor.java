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
import org.apache.storm.utils.NimbusClient;

import java.util.Map;

public class Monitor {

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("i", "interval", 4, CLI.AS_INT)
            .opt("m", "component", null)
            .opt("s", "stream", "default")
            .opt("w", "watch", "emitted")
            .arg("topologyName", CLI.FIRST_WINS)
            .parse(args);
        final org.apache.storm.utils.Monitor monitor = new org.apache.storm.utils.Monitor();
        Integer interval = (Integer) cl.get("i");
        String component = (String) cl.get("m");
        String stream = (String) cl.get("s");
        String watch = (String) cl.get("w");
        String topologyName = (String) cl.get("topologyName");

        if (null != interval) {
            monitor.set_interval(interval);
        }
        if (null != component) {
            monitor.set_component(component);
        }
        if (null != stream) {
            monitor.set_stream(stream);
        }
        if (null != watch) {
            monitor.set_watch(watch);
        }
        if (null != topologyName) {
            monitor.set_topology(topologyName);
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Client nimbus) throws Exception {
                monitor.metrics(nimbus);
            }
        });
    }
}
