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

import org.apache.storm.trident.state.ValueUpdater;

import java.util.ArrayList;
import java.util.List;


public class NonTransactionalMap<T> implements MapState<T> {
    public static <T> MapState<T> build(IBackingMap<T> backing) {
        return new NonTransactionalMap<T>(backing);
    }
    
    IBackingMap<T> _backing;
    
    protected NonTransactionalMap(IBackingMap<T> backing) {
        _backing = backing;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _backing.multiGet(keys);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<T> curr = _backing.multiGet(keys);
        List<T> ret = new ArrayList<T>(curr.size());
        for(int i=0; i<curr.size(); i++) {
            T currVal = curr.get(i);
            ValueUpdater<T> updater = updaters.get(i);
            ret.add(updater.update(currVal));
        }
        _backing.multiPut(keys, ret);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _backing.multiPut(keys, vals);
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
    }  
}
