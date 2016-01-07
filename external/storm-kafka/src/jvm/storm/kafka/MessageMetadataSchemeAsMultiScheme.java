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
package storm.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import backtype.storm.spout.SchemeAsMultiScheme;

public class MessageMetadataSchemeAsMultiScheme extends SchemeAsMultiScheme {
    private static final long serialVersionUID = -7172403703813625116L;

    public MessageMetadataSchemeAsMultiScheme(MessageMetadataScheme scheme) {
        super(scheme);
    }

    public Iterable<List<Object>> deserializeMessageWithMetadata(ByteBuffer message, Partition partition, long offset) {
        List<Object> o = ((MessageMetadataScheme) scheme).deserializeMessageWithMetadata(message, partition, offset);
        if (o == null) {
            return null;
        } else {
            return Arrays.asList(o);
        }
    }
}
