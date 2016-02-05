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
package org.apache.storm.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.io.OutputStream;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

/*
 * Listens for all metrics, dumps them as text to a configured host:port
 *
 * To use, add this to your topology's configuration:
 *
 * ```java
 *   conf.registerMetricsConsumer(org.apache.storm.testing.ForwardingMetricsConsumer.class, "<HOST>:<PORT>", 1);
 * ```
 *
 * Or edit the storm.yaml config file:
 *
 * ```yaml
 *   topology.metrics.consumer.register:
 *     - class: "org.apache.storm.testing.ForwardingMetricsConsumer"
 *     - argument: "example.com:9999"
 *       parallelism.hint: 1
 * ```
 *
 */
public class ForwardingMetricsConsumer implements IMetricsConsumer {
    String host;
    int port;
    Socket socket;
    OutputStream out;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        String [] parts = ((String)registrationArgument).split(":",2);
        host = parts[0];
        port = Integer.valueOf(parts[1]);
        try {
          socket = new Socket(host, port);
          out = socket.getOutputStream();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = taskInfo.timestamp + "\t" +
            taskInfo.srcWorkerHost + ":"+ taskInfo.srcWorkerPort + "\t"+
            taskInfo.srcTaskId + "\t" + taskInfo.srcComponentId + "\t";
        sb.append(header);
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            sb.append(p.name)
                .append("\t")
                .append(p.value)
                .append("\n");
            try {
              out.write(sb.toString().getBytes());
              out.flush();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void cleanup() { 
      try {
        socket.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
}
