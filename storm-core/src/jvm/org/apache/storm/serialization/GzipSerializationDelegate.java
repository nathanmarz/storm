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

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Note, this assumes it's deserializing a gzip byte stream, and will err if it encounters any other serialization.
 */
public class GzipSerializationDelegate implements SerializationDelegate {

    @Override
    public void prepare(Map stormConf) {
        // No-op
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gos = new GZIPOutputStream(bos);
            ObjectOutputStream oos = new ObjectOutputStream(gos);
            oos.writeObject(object);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            GZIPInputStream gis = new GZIPInputStream(bis);
            ObjectInputStream ois = new ObjectInputStream(gis);
            Object ret = ois.readObject();
            ois.close();
            return (T)ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
