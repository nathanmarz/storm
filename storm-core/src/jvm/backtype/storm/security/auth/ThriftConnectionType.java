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

import backtype.storm.utils.Utils;
import backtype.storm.Config;

import java.util.Map;

/**
 * The purpose for which the Thrift server is created.
 */
public enum ThriftConnectionType {
    NIMBUS(Config.NIMBUS_THRIFT_TRANSPORT_PLUGIN, Config.NIMBUS_THRIFT_PORT, null,
         Config.NIMBUS_THRIFT_THREADS, Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE),
    DRPC(Config.DRPC_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_PORT, Config.DRPC_QUEUE_SIZE,
         Config.DRPC_WORKER_THREADS, Config.DRPC_MAX_BUFFER_SIZE),
    DRPC_INVOCATIONS(Config.DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_INVOCATIONS_PORT, null,
         Config.DRPC_INVOCATIONS_THREADS, Config.DRPC_MAX_BUFFER_SIZE);

    private final String _transConf;
    private final String _portConf;
    private final String _qConf;
    private final String _threadsConf;
    private final String _buffConf;

    ThriftConnectionType(String transConf, String portConf, String qConf,
                         String threadsConf, String buffConf) {
        _transConf = transConf;
        _portConf = portConf;
        _qConf = qConf;
        _threadsConf = threadsConf;
        _buffConf = buffConf;
    }

    public String getTransportPlugin(Map conf) {
        String ret = (String)conf.get(_transConf);
        if (ret == null) {
            ret = (String)conf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN);
        }
        return ret;
    }

    public int getPort(Map conf) {
        return Utils.getInt(conf.get(_portConf));
    }

    public Integer getQueueSize(Map conf) {
        if (_qConf == null) {
            return null;
        }
        return (Integer)conf.get(_qConf);
    }

    public int getNumThreads(Map conf) { 
        return Utils.getInt(conf.get(_threadsConf));
    }

    public int getMaxBufferSize(Map conf) {
        return Utils.getInt(conf.get(_buffConf));
    }
}
