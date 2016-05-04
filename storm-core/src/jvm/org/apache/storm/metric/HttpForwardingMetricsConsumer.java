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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.net.URL;
import java.net.HttpURLConnection;

import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.serialization.KryoValuesSerializer;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

/**
 * Listens for all metrics and POSTs them serialized to a configured URL
 *
 * To use, add this to your topology's configuration:
 *
 * ```java
 *   conf.registerMetricsConsumer(org.apache.storm.metrics.HttpForwardingMetricsConsumer.class, "http://example.com:8080/metrics/my-topology/", 1);
 * ```
 *
 * The body of the post is data serialized using {@link org.apache.storm.serialization.KryoValuesSerializer}, with the data passed in
 * as a list of `[TaskInfo, Collection<DataPoint>]`.  More things may be appended to the end of the list in the future.
 *
 * The values can be deserialized using the org.apache.storm.serialization.KryoValuesDeserializer, and a 
 * correct config + classpath.
 *
 * @see org.apache.storm.serialization.KryoValuesSerializer
 */
public class HttpForwardingMetricsConsumer implements IMetricsConsumer {
    private transient URL _url; 
    private transient IErrorReporter _errorReporter;
    private transient KryoValuesSerializer _serializer;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) { 
        try {
            _url = new URL((String)registrationArgument);
            _errorReporter = errorReporter;
            _serializer = new KryoValuesSerializer(stormConf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        try {
            HttpURLConnection con = (HttpURLConnection)_url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            Output out = new Output(con.getOutputStream());
            _serializer.serializeInto(Arrays.asList(taskInfo, dataPoints), out);
            out.flush();
            out.close();
            //The connection is not sent unless a response is requested
            int response = con.getResponseCode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() { }
}
