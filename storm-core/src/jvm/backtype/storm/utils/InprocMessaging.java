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
package backtype.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class InprocMessaging {
    private static Map<Integer, LinkedBlockingQueue<Object>> _queues = new HashMap<Integer, LinkedBlockingQueue<Object>>();
    private static final Object _lock = new Object();
    private static int port = 1;
    
    public static int acquireNewPort() {
        int ret;
        synchronized(_lock) {
            ret = port;
            port++;
        }
        return ret;
    }
    
    public static void sendMessage(int port, Object msg) {
        getQueue(port).add(msg);
    }
    
    public static Object takeMessage(int port) throws InterruptedException {
        return getQueue(port).take();
    }

    public static Object pollMessage(int port) {
        return  getQueue(port).poll();
    }    
    
    private static LinkedBlockingQueue<Object> getQueue(int port) {
        synchronized(_lock) {
            if(!_queues.containsKey(port)) {
              _queues.put(port, new LinkedBlockingQueue<Object>());   
            }
            return _queues.get(port);
        }
    }

}
