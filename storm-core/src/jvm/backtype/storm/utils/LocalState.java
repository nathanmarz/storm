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
package backtype.storm.utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import backtype.storm.generated.LocalStateData;
import backtype.storm.generated.ThriftSerializedObject;

/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class LocalState {
    public static Logger LOG = LoggerFactory.getLogger(LocalState.class);
    private VersionedStore _vs;
    
    public LocalState(String backingDir) throws IOException {
        LOG.debug("New Local State for {}", backingDir);
        _vs = new VersionedStore(backingDir);
    }

    public synchronized Map<String, TBase> snapshot() {
        int attempts = 0;
        while(true) {
            try {
                return deserializeLatestVersion();
            } catch (Exception e) {
                attempts++;
                if (attempts >= 10) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Map<String, TBase> deserializeLatestVersion() throws IOException {
        Map<String, TBase> result = new HashMap<String, TBase>();
        TDeserializer td = new TDeserializer();
        for (Map.Entry<String, ThriftSerializedObject> ent: partialDeserializeLatestVersion(td).entrySet()) {
            result.put(ent.getKey(), deserialize(ent.getValue(), td));
        }
        return result;
    }

    private TBase deserialize(ThriftSerializedObject obj, TDeserializer td) {
        try {
            Class<?> clazz = Class.forName(obj.get_name());
            TBase instance = (TBase) clazz.newInstance();
            td.deserialize(instance, obj.get_bits());
            return instance;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ThriftSerializedObject> partialDeserializeLatestVersion(TDeserializer td) {
        try {
            String latestPath = _vs.mostRecentVersionPath();
            Map<String, ThriftSerializedObject> result = new HashMap<String, ThriftSerializedObject>();
            if (latestPath != null) {
                byte[] serialized = FileUtils.readFileToByteArray(new File(latestPath));
                if (serialized.length == 0) {
                    LOG.warn("LocalState file '{}' contained no data, resetting state", latestPath);
                } else {
                    if (td == null) {
                        td = new TDeserializer();
                    }
                    LocalStateData data = new LocalStateData();
                    td.deserialize(data, serialized);
                    result = data.get_serialized_parts();
                }
            }
            return result;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized Map<String, ThriftSerializedObject> partialSnapshot(TDeserializer td) {
        int attempts = 0;
        while(true) {
            try {
                return partialDeserializeLatestVersion(td);
            } catch (Exception e) {
                attempts++;
                if (attempts >= 10) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public TBase get(String key) {
        TDeserializer td = new TDeserializer();
        Map<String, ThriftSerializedObject> partial = partialSnapshot(td);
        ThriftSerializedObject tso = partial.get(key);
        TBase ret = null;
        if (tso != null) {
            ret = deserialize(tso, td);
        }
        return ret;
    }
    
    public void put(String key, TBase val) {
        put(key, val, true);
    }

    public synchronized void put(String key, TBase val, boolean cleanup) {
        Map<String, ThriftSerializedObject> curr = partialSnapshot(null);
        TSerializer ser = new TSerializer();
        curr.put(key, serialize(val, ser));
        persistInternal(curr, ser, cleanup);
    }

    public void remove(String key) {
        remove(key, true);
    }

    public synchronized void remove(String key, boolean cleanup) {
        Map<String, ThriftSerializedObject> curr = partialSnapshot(null);
        curr.remove(key);
        persistInternal(curr, null, cleanup);
    }

    public synchronized void cleanup(int keepVersions) throws IOException {
        _vs.cleanup(keepVersions);
    }

    private void persistInternal(Map<String, ThriftSerializedObject> serialized, TSerializer ser, boolean cleanup) {
        try {
            if (ser == null) {
                ser = new TSerializer();
            }
            byte[] toWrite = ser.serialize(new LocalStateData(serialized));

            String newPath = _vs.createVersion();
            File file = new File(newPath);
            FileUtils.writeByteArrayToFile(file, toWrite);
            if (toWrite.length != file.length()) {
                throw new IOException("Tried to serialize " + toWrite.length + 
                        " bytes to " + file.getCanonicalPath() + ", but " +
                        file.length() + " bytes were written.");
            }
            _vs.succeedVersion(newPath);
            if(cleanup) _vs.cleanup(4);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ThriftSerializedObject serialize(TBase o, TSerializer ser) {
        try {
            return new ThriftSerializedObject(o.getClass().getName(), ByteBuffer.wrap(ser.serialize((TBase)o)));
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void persist(Map<String, TBase> val, boolean cleanup) {
        try {
            TSerializer ser = new TSerializer();
            Map<String, ThriftSerializedObject> serialized = new HashMap<String, ThriftSerializedObject>();
            for (Map.Entry<String, TBase> ent: val.entrySet()) {
                Object o = ent.getValue();
                serialized.put(ent.getKey(), serialize(ent.getValue(), ser));
            }
            persistInternal(serialized, ser, cleanup);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
