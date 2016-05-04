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
package org.apache.storm.trident.operation.impl;

import java.util.List;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.ComboList;

public class GroupCollector implements TridentCollector {
    public List<Object> currGroup;
    
    ComboList.Factory _factory;
    TridentCollector _collector;
    
    public GroupCollector(TridentCollector collector, ComboList.Factory factory) {
        _factory = factory;
        _collector = collector;
    }
    
    @Override
    public void emit(List<Object> values) {
        List[] delegates = new List[2];
        delegates[0] = currGroup;
        delegates[1] = values;
        _collector.emit(_factory.create(delegates));
    }

    @Override
    public void reportError(Throwable t) {
        _collector.reportError(t);
    }
    
}
