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
package org.apache.storm.serialization;

import org.apache.storm.utils.ListDelegate;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoValuesDeserializer {
    Kryo _kryo;
    Input _kryoInput;
    
    public KryoValuesDeserializer(Map conf) {
        _kryo = SerializationFactory.getKryo(conf);
        _kryoInput = new Input(1);
    }
    
    public List<Object> deserializeFrom(Input input) {
    	ListDelegate delegate = _kryo.readObject(input, ListDelegate.class);
   	return delegate.getDelegate();
    }
    
    public List<Object> deserialize(byte[] ser) throws IOException {
        _kryoInput.setBuffer(ser);
        return deserializeFrom(_kryoInput);
    }
    
    public Object deserializeObject(byte[] ser) throws IOException {
        _kryoInput.setBuffer(ser);
        return _kryo.readClassAndObject(_kryoInput);
    }
}
