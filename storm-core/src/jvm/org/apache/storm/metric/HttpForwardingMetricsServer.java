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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.net.ServerSocket;
import java.net.InetAddress;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;

import com.esotericsoftware.kryo.io.Input;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.utils.Utils;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * A server that can listen for metrics from the HttpForwardingMetricsConsumer.
 */
public abstract class HttpForwardingMetricsServer {
    private Map _conf;
    private Server _server = null;
    private int _port = -1;
    private String _url = null;

    ThreadLocal<KryoValuesDeserializer> _des = new ThreadLocal<KryoValuesDeserializer>() {
        @Override
        protected KryoValuesDeserializer initialValue() {
            return new KryoValuesDeserializer(_conf);
        }
    };

    private class MetricsCollectionServlet extends HttpServlet
    {
        protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
        {
            Input in = new Input(request.getInputStream());
            List<Object> metrics = _des.get().deserializeFrom(in);
            handle((TaskInfo)metrics.get(0), (Collection<DataPoint>)metrics.get(1));
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    public HttpForwardingMetricsServer(Map conf) {
        _conf = Utils.readStormConfig();
        if (conf != null) {
            _conf.putAll(conf);
        }
    }

    //This needs to be thread safe
    public abstract void handle(TaskInfo taskInfo, Collection<DataPoint> dataPoints);

    public void serve(Integer port) {
        try {
            if (_server != null) throw new RuntimeException("The server is already running");
    
            if (port == null || port <= 0) {
                ServerSocket s = new ServerSocket(0);
                port = s.getLocalPort();
                s.close();
            }
            _server = new Server(port);
            _port = port;
            _url = "http://"+InetAddress.getLocalHost().getHostName()+":"+_port+"/";
 
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            _server.setHandler(context);
 
            context.addServlet(new ServletHolder(new MetricsCollectionServlet()),"/*");

            _server.start();
         } catch (RuntimeException e) {
             throw e;
         } catch (Exception e) {
             throw new RuntimeException(e);
         }
    }

    public void serve() {
        serve(null);
    }

    public int getPort() {
        return _port;
    }

    public String getUrl() {
        return _url;
    }
}
