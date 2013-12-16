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


import clojure.lang.ILookup;
import clojure.lang.ISeq;
import clojure.lang.AFn;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.Keyword;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.util.Set;

public class IndifferentAccessMap extends AFn implements ILookup, IPersistentMap, Map {

    protected IPersistentMap _map;

    protected IndifferentAccessMap() {
    }

    public IndifferentAccessMap(IPersistentMap map) {
        setMap(map);
    }

    public IPersistentMap getMap() {
        return _map;
    }

    public IPersistentMap setMap(IPersistentMap map) {
        _map = map;
        return _map;
    }

    public int size() {
        return ((Map) getMap()).size();
    }

    public int count() {
        return size();
    }

    public ISeq seq() {
        return getMap().seq();
    }

    @Override
    public Object valAt(Object o) {
        if(o instanceof Keyword) {
            return valAt(((Keyword) o).getName());
        }
        return getMap().valAt(o);
    }
    
    @Override
    public Object valAt(Object o, Object def) {
        Object ret = valAt(o);
        if(ret==null) ret = def;
        return ret;
    }

    /* IFn */
    @Override
    public Object invoke(Object o) {
        return valAt(o);
    }

    @Override
    public Object invoke(Object o, Object notfound) {
        return valAt(o, notfound);
    }

    /* IPersistentMap */
    /* Naive implementation, but it might be good enough */
    public IPersistentMap assoc(Object k, Object v) {
        if(k instanceof Keyword) return assoc(((Keyword) k).getName(), v);
        
        return new IndifferentAccessMap(getMap().assoc(k, v));
    }

    public IPersistentMap assocEx(Object k, Object v) {
        if(k instanceof Keyword) return assocEx(((Keyword) k).getName(), v);

        return new IndifferentAccessMap(getMap().assocEx(k, v));
    }

    public IPersistentMap without(Object k) {
        if(k instanceof Keyword) return without(((Keyword) k).getName());

        return new IndifferentAccessMap(getMap().without(k));
    }

    public boolean containsKey(Object k) {
        if(k instanceof Keyword) return containsKey(((Keyword) k).getName());
        return getMap().containsKey(k);
    }

    public IMapEntry entryAt(Object k) {
        if(k instanceof Keyword) return entryAt(((Keyword) k).getName());

        return getMap().entryAt(k);
    }

    public IPersistentCollection cons(Object o) {
        return getMap().cons(o);
    }

    public IPersistentCollection empty() {
        return new IndifferentAccessMap(PersistentArrayMap.EMPTY);
    }

    public boolean equiv(Object o) {
        return getMap().equiv(o);
    }

    public Iterator iterator() {
        return getMap().iterator();
    }

    /* Map */
    public boolean containsValue(Object v) {
        return ((Map) getMap()).containsValue(v);
    }

    public Set entrySet() {
        return ((Map) getMap()).entrySet();
    }

    public Object get(Object k) {
        return valAt(k);
    }

    public boolean isEmpty() {
        return ((Map) getMap()).isEmpty();
    }

    public Set keySet() {
        return ((Map) getMap()).keySet();
    }

    public Collection values() {
        return ((Map) getMap()).values();
    }
    
    /* Not implemented */
    public void clear() {
        throw new UnsupportedOperationException();
    }
    public Object put(Object k, Object v) {
        throw new UnsupportedOperationException();
    }
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }
    public Object remove(Object k) {
        throw new UnsupportedOperationException();
    }
}
