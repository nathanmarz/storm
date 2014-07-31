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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Always writes gzip out, but tests incoming to see if it's gzipped. If it is, deserializes with gzip. If not, uses
 * {@link backtype.storm.serialization.DefaultSerializationDelegate} to deserialize. Any logic needing to be enabled
 * via {@link #prepare(java.util.Map)} is passed through to both delegates.
 * @author danehammer
 */
public class GzipBridgeSerializationDelegate implements SerializationDelegate {

    private DefaultSerializationDelegate defaultDelegate = new DefaultSerializationDelegate();
    private GzipSerializationDelegate gzipDelegate = new GzipSerializationDelegate();

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
    public Object deserialize(byte[] bytes) {
        if (isGzipped(bytes)) {
            return gzipDelegate.deserialize(bytes);
        } else {
            return defaultDelegate.deserialize(bytes);
        }
    }

    /**
     * Looks ahead to see if the GZIP magic constant is heading {@code bytes}
     */
    private boolean isGzipped(byte[] bytes) {
        // Split up GZIP_MAGIC into readable bytes
        byte magicFirst = (byte) GZIPInputStream.GZIP_MAGIC;
        byte magicSecond =(byte) (GZIPInputStream.GZIP_MAGIC >> 8);

        return (bytes[0] == magicFirst) && (bytes[1] == magicSecond);
    }
}
