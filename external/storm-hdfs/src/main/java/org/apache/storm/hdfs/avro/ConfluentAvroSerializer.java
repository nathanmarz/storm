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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;

/**
 * This class provides a mechanism to utilize the Confluent Schema Registry (https://github.com/confluentinc/schema-registry)
 * for Storm to (de)serialize Avro generic records across a topology.  It assumes the schema registry is up and running
 * completely independent of Storm.
 */
public class ConfluentAvroSerializer extends AbstractAvroSerializer {

    private SchemaRegistryClient theClient;
    final private String url;

    /**
     * A constructor for use by test cases ONLY, thus the default scope.
     * @param url The complete URL reference of a confluent schema registry, e.g. "http://HOST:PORT"
     */
    ConfluentAvroSerializer(String url) {
        this.url = url;
        this.theClient = new CachedSchemaRegistryClient(this.url, 10000);
    }

    /**
     * A constructor with a signature that Storm can locate and use with kryo registration.
     * See Storm's SerializationFactory class for details
     *
     * @param k Unused but needs to be present for Serialization Factory to find this constructor
     * @param stormConf The global storm configuration. Must define "avro.schemaregistry.confluent" to locate the
     *                  confluent schema registry. Should in the form of "http://HOST:PORT"
     */
    public ConfluentAvroSerializer(Kryo k, Map stormConf) {
        url = (String) stormConf.get("avro.schemaregistry.confluent");
        this.theClient = new CachedSchemaRegistryClient(this.url, 10000);
    }

    @Override
    public String getFingerprint(Schema schema) {
        final String subject = schema.getName();
        final int guid;
        try {
            guid = theClient.register(subject, schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
        return Integer.toString(guid);
    }

    @Override
    public Schema getSchema(String fingerPrint) {
        final Schema theSchema;
        try {
            theSchema = theClient.getByID(Integer.parseInt(fingerPrint));
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
        return theSchema;
    }
}
