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

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;


/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class LocalState {
    private VersionedStore _vs;
    
    public LocalState(String backingDir) throws IOException {
        _vs = new VersionedStore(backingDir);
    }
    
    public synchronized Map<Object, Object> snapshot() throws IOException {
        int attempts = 0;
        while(true) {
            String latestPath = _vs.mostRecentVersionPath();
            if(latestPath==null) return new HashMap<Object, Object>();
            try {
                return (Map<Object, Object>) Utils.deserialize(FileUtils.readFileToByteArray(new File(latestPath)));
            } catch(IOException e) {
                attempts++;
                if(attempts >= 10) {
                    throw e;
                }
            }
        }
    }
    
    public Object get(Object key) throws IOException {
        return snapshot().get(key);
    }
    
    public synchronized void put(Object key, Object val) throws IOException {
        put(key, val, true);
    }

    public synchronized void put(Object key, Object val, boolean cleanup) throws IOException {
        Map<Object, Object> curr = snapshot();
        curr.put(key, val);
        persist(curr, cleanup);
    }

    public synchronized void remove(Object key) throws IOException {
        remove(key, true);
    }

    public synchronized void remove(Object key, boolean cleanup) throws IOException {
        Map<Object, Object> curr = snapshot();
        curr.remove(key);
        persist(curr, cleanup);
    }

    public synchronized void cleanup(int keepVersions) throws IOException {
        _vs.cleanup(keepVersions);
    }
    
    private void persist(Map<Object, Object> val, boolean cleanup) throws IOException {
        byte[] toWrite = Utils.serialize(val);
        String newPath = _vs.createVersion();
        FileUtils.writeByteArrayToFile(new File(newPath), toWrite);
        _vs.succeedVersion(newPath);
        if(cleanup) _vs.cleanup(4);
    }
}