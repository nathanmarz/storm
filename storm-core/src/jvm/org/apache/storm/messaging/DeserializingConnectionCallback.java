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
package org.apache.storm.messaging;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.serialization.KryoTupleDeserializer;

import clojure.lang.IFn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A class that is called when a TaskMessage arrives.
 */
public class DeserializingConnectionCallback implements IConnectionCallback {
    private final IFn _cb;
    private final Map _conf;
    private final GeneralTopologyContext _context;
    private final ThreadLocal<KryoTupleDeserializer> _des =
         new ThreadLocal<KryoTupleDeserializer>() {
             @Override
             protected KryoTupleDeserializer initialValue() {
                 return new KryoTupleDeserializer(_conf, _context);
             }
         };

    public DeserializingConnectionCallback(final Map conf, final GeneralTopologyContext context, IFn callback) {
        _conf = conf;
        _context = context;
        _cb = callback;
    }

    @Override
    public void recv(List<TaskMessage> batch) {
        KryoTupleDeserializer des = _des.get();
        ArrayList<AddressedTuple> ret = new ArrayList<>(batch.size());
        for (TaskMessage message: batch) {
            ret.add(new AddressedTuple(message.task(), des.deserialize(message.message())));
        }
        _cb.invoke(ret);
    }
}
