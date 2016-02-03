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

/**
 * A default implementation of the AvroSerializer that will just pass literal schemas back and forth.  This should
 * only be used if no other serializer will fit a use case.
 */
public class GenericAvroSerializer extends AbstractAvroSerializer {
    @Override
    public String getFingerprint(Schema schema) {
        return schema.toString();
    }

    @Override
    public Schema getSchema(String fingerPrint) {
        return new Schema.Parser().parse(fingerPrint);
    }
}