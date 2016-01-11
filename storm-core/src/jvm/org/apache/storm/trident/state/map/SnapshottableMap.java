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

import java.util.Arrays;
import java.util.List;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.snapshot.Snapshottable;


public class SnapshottableMap<T> implements MapState<T>, Snapshottable<T> {
    MapState<T> _delegate;
    List<List<Object>> _keys;

    public SnapshottableMap(MapState<T> delegate, List<Object> snapshotKey) {
        _delegate = delegate;
        _keys = Arrays.asList(snapshotKey);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    @Override
    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
    }

    @Override
    public void commit(Long txid) {
        _delegate.commit(txid);
    }

    @Override
    public T get() {
        return multiGet(_keys).get(0);
    }

    @Override
    public T update(ValueUpdater updater) {
        List<ValueUpdater> updaters = Arrays.asList(updater);
        return multiUpdate(_keys, updaters).get(0);
    }

    @Override
    public void set(T o) {
        multiPut(_keys, Arrays.asList(o));
    }
    
}
