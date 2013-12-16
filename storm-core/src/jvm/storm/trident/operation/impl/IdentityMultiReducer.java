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
package storm.trident.operation.impl;

import java.util.Map;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;


public class IdentityMultiReducer implements MultiReducer {

    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
    }

    @Override
    public Object init(TridentCollector collector) {
        return null;
    }

    @Override
    public void execute(Object state, int streamIndex, TridentTuple input, TridentCollector collector) {
        collector.emit(input);
    }

    @Override
    public void complete(Object state, TridentCollector collector) {
    }

    @Override
    public void cleanup() {
    }
    
}
