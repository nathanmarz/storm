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
package backtype.storm.serialization;

import backtype.storm.utils.ListDelegate;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoValuesSerializer {
    Kryo _kryo;
    ListDelegate _delegate;
    Output _kryoOut;
    
    public KryoValuesSerializer(Map conf) {
        _kryo = SerializationFactory.getKryo(conf);
        _delegate = new ListDelegate();
        _kryoOut = new Output(2000, 2000000000);
    }
    
    public void serializeInto(List<Object> values, Output out) throws IOException {
        // this ensures that list of values is always written the same way, regardless
        // of whether it's a java collection or one of clojure's persistent collections 
        // (which have different serializers)
        // Doing this lets us deserialize as ArrayList and avoid writing the class here
        _delegate.setDelegate(values);
        _kryo.writeObject(out, _delegate); 
    }
    
    public byte[] serialize(List<Object> values) throws IOException {
        _kryoOut.clear();
        serializeInto(values, _kryoOut);
        return _kryoOut.toBytes();
    }
    
    public byte[] serializeObject(Object obj) {
        _kryoOut.clear();
        _kryo.writeClassAndObject(_kryoOut, obj);
        return _kryoOut.toBytes();
    }
}
