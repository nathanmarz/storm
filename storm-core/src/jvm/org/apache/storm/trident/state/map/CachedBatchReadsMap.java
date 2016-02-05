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
package org.apache.storm.trident.state.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.state.ValueUpdater;


public class CachedBatchReadsMap<T> {
    public static class RetVal<T> {
        public boolean cached;
        public T val;

        public RetVal(T v, boolean c) {
            val = v;
            cached = c;
        }
    }

    Map<List<Object>, T> _cached = new HashMap<List<Object>, T>();
    
    public IBackingMap<T> _delegate;
    
    public CachedBatchReadsMap(IBackingMap<T> delegate) {
        _delegate = delegate;
    }

    public void reset() {
        _cached.clear();
    }
    
    public List<RetVal<T>> multiGet(List<List<Object>> keys) {
        // TODO: can optimize further by only querying backing map for keys not in the cache
        List<T> vals = _delegate.multiGet(keys);
        List<RetVal<T>> ret = new ArrayList(vals.size());
        for(int i=0; i<keys.size(); i++) {
            List<Object> key = keys.get(i);
            if(_cached.containsKey(key)) {
                ret.add(new RetVal(_cached.get(key), true));
            } else {
                ret.add(new RetVal(vals.get(i), false));
            }
        }
        return ret;
    }

    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
        cache(keys, vals);
    }
    
    private void cache(List<List<Object>> keys, List<T> vals) {
        for(int i=0; i<keys.size(); i++) {
            List<Object> key = keys.get(i);
            T val = vals.get(i);
            _cached.put(key, val);
        }
    }


    
}
