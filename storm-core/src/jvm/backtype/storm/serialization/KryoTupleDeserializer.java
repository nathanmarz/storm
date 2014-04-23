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

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.WritableUtils;
import com.esotericsoftware.kryo.io.Input;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;
    
    public KryoTupleDeserializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        _kryoInput = new Input(1);
    }        

    public Tuple deserialize(byte[] ser) {
        try {
            _kryoInput.setBuffer(ser);
            int taskId = _kryoInput.readInt(true);
            int streamId = _kryoInput.readInt(true);
            String componentName = _context.getComponentId(taskId);
            String streamName = _ids.getStreamName(componentName, streamId);
            MessageId id = MessageId.deserialize(_kryoInput);
            List<Object> values = _kryo.deserializeFrom(_kryoInput);
            return new TupleImpl(_context, values, taskId, streamName, id);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
