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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AckTracker implements AckFailDelegate {
    private static Map<String, AtomicInteger> acks = new ConcurrentHashMap<String, AtomicInteger>();
    
    private String _id;
    
    public AckTracker() {
        _id = UUID.randomUUID().toString();
        acks.put(_id, new AtomicInteger(0));
    }
    
    @Override
    public void ack(Object id) {
        acks.get(_id).incrementAndGet();
    }

    @Override
    public void fail(Object id) {
    }
    
    public int getNumAcks() {
        return acks.get(_id).intValue();
    }
    
    public void resetNumAcks() {
        acks.get(_id).set(0);
    }
    
}
