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
package org.apache.storm.hdfs.avro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;

//Generously adapted from:
//https://github.com/kijiproject/kiji-express/blob/master/kiji-express/src/main/scala/org/kiji/express/flow/framework/serialization/AvroSerializer.scala
//Which has as an ASL2.0 license

/**
 * This abstract class can be extended to implement concrete classes capable of (de)serializing generic avro objects
 * across a Topology.  The methods in the AvroSchemaRegistry interface specify how schemas can be mapped to unique
 * identifiers and vice versa.  Implementations based on pre-defining schemas or utilizing an external schema registry
 * are provided.
 */
public abstract class AbstractAvroSerializer extends Serializer<GenericContainer> implements AvroSchemaRegistry {

    @Override
    public void write(Kryo kryo, Output output, GenericContainer record) {

        String fingerPrint = this.getFingerprint(record.getSchema());
        output.writeString(fingerPrint);
        GenericDatumWriter<GenericContainer> writer = new GenericDatumWriter<>(record.getSchema());

        BinaryEncoder encoder = EncoderFactory
                .get()
                .directBinaryEncoder(output, null);
        try {
            writer.write(record, encoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericContainer read(Kryo kryo, Input input, Class<GenericContainer> aClass) {
        Schema theSchema = this.getSchema(input.readString());
        GenericDatumReader<GenericContainer> reader = new GenericDatumReader<>(theSchema);
        Decoder decoder = DecoderFactory
                .get()
                .directBinaryDecoder(input, null);

        GenericContainer foo;
        try {
            foo = reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return foo;
    }
}
