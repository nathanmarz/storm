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
package org.apache.storm.serialization;

import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Always writes gzip out, but tests incoming to see if it's gzipped. If it is, deserializes with gzip. If not, uses
 * {@link org.apache.storm.serialization.ThriftSerializationDelegate} to deserialize. Any logic needing to be enabled
 * via {@link #prepare(java.util.Map)} is passed through to both delegates.
 */
public class GzipBridgeThriftSerializationDelegate implements SerializationDelegate {

    private ThriftSerializationDelegate defaultDelegate = new ThriftSerializationDelegate();
    private GzipThriftSerializationDelegate gzipDelegate = new GzipThriftSerializationDelegate();

    @Override
    public void prepare(Map stormConf) {
        defaultDelegate.prepare(stormConf);
        gzipDelegate.prepare(stormConf);
    }

    @Override
    public byte[] serialize(Object object) {
        return gzipDelegate.serialize(object);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if (isGzipped(bytes)) {
            return gzipDelegate.deserialize(bytes, clazz);
        } else {
            return defaultDelegate.deserialize(bytes,clazz);
        }
    }

    // Split up GZIP_MAGIC into readable bytes
    private static final byte GZIP_MAGIC_FIRST_BYTE = (byte) GZIPInputStream.GZIP_MAGIC;
    private static final byte GZIP_MAGIC_SECOND_BYTE = (byte) (GZIPInputStream.GZIP_MAGIC >> 8);

    /**
     * Looks ahead to see if the GZIP magic constant is heading {@code bytes}
     */
    private boolean isGzipped(byte[] bytes) {
        return (bytes.length > 1) && (bytes[0] == GZIP_MAGIC_FIRST_BYTE)
               && (bytes[1] == GZIP_MAGIC_SECOND_BYTE);
    }
}
