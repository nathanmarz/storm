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
package backtype.storm.security.auth;

import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.utils.Utils;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private Map _storm_conf; //storm configuration
    protected TProcessor _processor = null;
    private int _port = 0;
    private TServer _server = null;
    private Configuration _login_conf;
    
    public ThriftServer(Map storm_conf, TProcessor processor, int port) {
        try {
            _storm_conf = storm_conf;
            _processor = processor;
            _port = port;
            
            //retrieve authentication configuration 
            _login_conf = AuthUtils.GetConfiguration(_storm_conf);
        } catch (Exception x) {
            LOG.error(x.getMessage(), x);
        }
    }

    public void stop() {
        if (_server != null)
            _server.stop();
    }

    /**
     * Is ThriftServer listening to requests?
     * @return
     */
    public boolean isServing() {
        if (_server == null) return false;
        return _server.isServing();
    }
    
    public void serve()  {
        try {
            //locate our thrift transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(_storm_conf, _login_conf);

            //server
            _server = transportPlugin.getServer(_port, _processor);

            //start accepting requests
            _server.serve();
        } catch (Exception ex) {
            LOG.error("ThriftServer is being stopped due to: " + ex, ex);
            if (_server != null) _server.stop();
            Runtime.getRuntime().halt(1); //shutdown server process since we could not handle Thrift requests any more
        }
    }
}
