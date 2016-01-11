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
package org.apache.storm.kafka;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

public class StringMessageAndMetadataScheme extends StringScheme implements MessageMetadataScheme {
    private static final long serialVersionUID = -5441841920447947374L;

    public static final String STRING_SCHEME_PARTITION_KEY = "partition";
    public static final String STRING_SCHEME_OFFSET = "offset";

    @Override
    public List<Object> deserializeMessageWithMetadata(ByteBuffer message, Partition partition, long offset) {
        String stringMessage = StringScheme.deserializeString(message);
        return new Values(stringMessage, partition.partition, offset);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY, STRING_SCHEME_PARTITION_KEY, STRING_SCHEME_OFFSET);
    }

}
