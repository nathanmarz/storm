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
package org.apache.storm.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class ListDelegate implements List<Object> {
    private List<Object> _delegate;
    
    public ListDelegate() {
    	_delegate = new ArrayList<>();
    }
    
    public void setDelegate(List<Object> delegate) {
        _delegate = delegate;
    }

    public List<Object> getDelegate() {
        return _delegate;
    }
    
    @Override
    public int size() {
        return _delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return _delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return _delegate.contains(o);
    }

    @Override
    public Iterator<Object> iterator() {
        return _delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return _delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return _delegate.toArray(ts);
    }

    @Override
    public boolean add(Object e) {
        return _delegate.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return _delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        return _delegate.containsAll(clctn);
    }

    @Override
    public boolean addAll(Collection<?> clctn) {
        return _delegate.addAll(clctn);
    }

    @Override
    public boolean addAll(int i, Collection<?> clctn) {
        return _delegate.addAll(i, clctn);
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        return _delegate.removeAll(clctn);
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        return _delegate.retainAll(clctn);
    }

    @Override
    public void clear() {
        _delegate.clear();
    }

    @Override
    public Object get(int i) {
        return _delegate.get(i);
    }

    @Override
    public Object set(int i, Object e) {
        return _delegate.set(i, e);
    }

    @Override
    public void add(int i, Object e) {
        _delegate.add(i, e);
    }

    @Override
    public Object remove(int i) {
        return _delegate.remove(i);
    }

    @Override
    public int indexOf(Object o) {
        return _delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return _delegate.lastIndexOf(o);
    }

    @Override
    public ListIterator<Object> listIterator() {
        return _delegate.listIterator();
    }

    @Override
    public ListIterator<Object> listIterator(int i) {
        return _delegate.listIterator(i);
    }

    @Override
    public List<Object> subList(int i, int i1) {
        return _delegate.subList(i, i1);
    }
    
}
