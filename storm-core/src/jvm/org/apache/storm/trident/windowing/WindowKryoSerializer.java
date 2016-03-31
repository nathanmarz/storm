/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.serialization.SerializationFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Kryo serializer/deserializer for values that are stored as part of windowing. This can be used in {@link WindowsStore}.
 * This class is not thread safe.
 *
 */
public class WindowKryoSerializer {

    private final Kryo kryo;
    private final Output output;
    private final Input input;

    public WindowKryoSerializer(Map stormConf) {
        kryo = SerializationFactory.getKryo(stormConf);
        output = new Output(2000, 2_000_000_000);
        input = new Input();
    }

    /**
     * Serializes the given object into a byte array using Kryo serialization.
     *
     * @param obj Object to be serialized.
     */
    public byte[] serialize(Object obj) {
        output.clear();
        kryo.writeClassAndObject(output, obj);
        return output.toBytes();
    }

    /**
     * Serializes the given object into a {@link ByteBuffer} backed by the byte array returned by Kryo serialization.
     *
     * @param obj Object to be serialized.
     */
    public ByteBuffer serializeToByteBuffer(Object obj) {
        output.clear();
        kryo.writeClassAndObject(output, obj);
        return ByteBuffer.wrap(output.getBuffer(), 0, output.position());
    }

    /**
     * Returns an Object which is created using Kryo deserialization of given byte array instance.
     *
     * @param buff byte array to be deserialized into an Object
     */
    public Object deserialize(byte[] buff) {
        input.setBuffer(buff);
        return kryo.readClassAndObject(input);
    }

    /**
     * Returns an Object which is created using Kryo deserialization of given {@code byteBuffer} instance.
     *
     * @param byteBuffer byte buffer to be deserialized into an Object
     */
    public Object deserialize(ByteBuffer byteBuffer) {
        input.setBuffer(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.position());
        return kryo.readClassAndObject(input);
    }
}
