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

import org.apache.avro.generic.GenericData;
import org.apache.storm.Config;

public class AvroUtils {
    /**
     * A helper method to extract avro serialization configurations from the topology configuration and register
     * specific kryo serializers as necessary.  A default serializer will be provided if none is specified in the
     * configuration.  "avro.serializer" should specify the complete class name of the serializer, e.g.
     * "org.apache.stgorm.hdfs.avro.GenericAvroSerializer"
     *
     * @param conf The topology configuration
     * @throws ClassNotFoundException If the specified serializer cannot be located.
     */
    public static void addAvroKryoSerializations(Config conf) throws ClassNotFoundException {
        final Class serializerClass;
        if (conf.containsKey("avro.serializer")) {
            serializerClass = Class.forName((String)conf.get("avro.serializer"));
        }
        else {
            serializerClass = GenericAvroSerializer.class;
        }
        conf.registerSerialization(GenericData.Record.class, serializerClass);
        conf.setSkipMissingKryoRegistrations(false);
    }
}
