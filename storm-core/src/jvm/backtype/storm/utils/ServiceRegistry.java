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
import java.util.UUID;

// this class should be combined with RegisteredGlobalState
public class ServiceRegistry {
    private static HashMap<String, Object> _services = new HashMap<String, Object>();
    private static final Object _lock = new Object();
    
    public static String registerService(Object service) {
        synchronized(_lock) {
            String id = UUID.randomUUID().toString();
            _services.put(id, service);
            return id;
        }
    }
    
    public static Object getService(String id) {
        synchronized(_lock) {
            return _services.get(id);
        }        
    }
    
    public static void unregisterService(String id) {
        synchronized(_lock) {
            _services.remove(id);
        }        
    }
}
