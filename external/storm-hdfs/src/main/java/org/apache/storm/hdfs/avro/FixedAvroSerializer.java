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

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * A class to help (de)serialize a pre-defined set of Avro schemas.  Schemas should be listed, one per line, in a file
 * called "FixedAvroSerializer.config", which must be part of the Storm topology jar file.  Any schemas intended to be
 * used with this class **MUST** be defined in that file.
 */
public class FixedAvroSerializer extends AbstractAvroSerializer {

    private final static String FP_ALGO = "CRC-64-AVRO";
    final Map<String, Schema> fingerprint2schemaMap = new HashMap<>();
    final Map<Schema, String> schema2fingerprintMap = new HashMap<>();

    public FixedAvroSerializer() throws IOException, NoSuchAlgorithmException {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("FixedAvroSerializer.config");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line;
        while((line = reader.readLine()) != null) {
            Schema schema = new Schema.Parser().parse(line);
            byte [] fp = SchemaNormalization.parsingFingerprint(FP_ALGO, schema);
            String fingerPrint = new String(Base64.decodeBase64(fp));

            fingerprint2schemaMap.put(fingerPrint, schema);
            schema2fingerprintMap.put(schema, fingerPrint);
        }
    }

    @Override
    public String getFingerprint(Schema schema) {
        return schema2fingerprintMap.get(schema);
    }

    @Override
    public Schema getSchema(String fingerPrint) {
        return fingerprint2schemaMap.get(fingerPrint);
    }
}
