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

import org.apache.storm.utils.RegisteredGlobalState;
import java.util.HashSet;
import java.util.Set;

public class AckFailMapTracker implements AckFailDelegate {
    
    String _acked;
    String _failed;
    
    public AckFailMapTracker() {
        _acked = RegisteredGlobalState.registerState(new HashSet());
        _failed = RegisteredGlobalState.registerState(new HashSet());
    }
    
    public boolean isAcked(Object id) {
        return ((Set)RegisteredGlobalState.getState(_acked)).contains(id);
    }
    
    public boolean isFailed(Object id) {
        return ((Set)RegisteredGlobalState.getState(_failed)).contains(id);        
    }

    @Override
    public void ack(Object id) {
        ((Set)RegisteredGlobalState.getState(_acked)).add(id);
    }

    @Override
    public void fail(Object id) {
        ((Set)RegisteredGlobalState.getState(_failed)).add(id);
    }
    
}
