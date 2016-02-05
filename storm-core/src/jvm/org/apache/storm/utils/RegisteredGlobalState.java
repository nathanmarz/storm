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
package org.apache.storm.utils;

import java.util.HashMap;
import java.util.UUID;

/**
 * This class is used as part of testing Storm. It is used to keep track of "global metrics"
 * in an atomic way. For example, it is used for doing fine-grained detection of when a 
 * local Storm cluster is idle by tracking the number of transferred tuples vs the number of processed
 * tuples.
 */
public class RegisteredGlobalState {
    private static HashMap<String, Object> _states = new HashMap<>();
    private static final Object _lock = new Object();
    
    public static Object globalLock() {
        return _lock;
    }
    
    public static String registerState(Object init) {
        synchronized(_lock) {
            String id = UUID.randomUUID().toString();
            _states.put(id, init);
            return id;
        }
    }
    
    public static void setState(String id, Object init) {
        synchronized(_lock) {
            _states.put(id, init);
        }
    }
    
    public static Object getState(String id) {
        synchronized(_lock) {
            return _states.get(id);
        }        
    }
    
    public static void clearState(String id) {
        synchronized(_lock) {
            _states.remove(id);
        }        
    }
}
