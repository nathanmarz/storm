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
package org.apache.storm.trident.util;

import java.io.Serializable;


public class IndexedEdge<T> implements Comparable, Serializable {
    public T source;
    public T target;
    public int index;
    
    public IndexedEdge(T source, T target, int index) {
        this.source = source;
        this.target = target;
        this.index = index;
    }

    @Override
    public int hashCode() {
        return 13* source.hashCode() + 7 * target.hashCode() + index;
    }

    @Override
    public boolean equals(Object o) {
        IndexedEdge other = (IndexedEdge) o;
        return source.equals(other.source) && target.equals(other.target) && index == other.index;
    }

    @Override
    public int compareTo(Object t) {
        IndexedEdge other = (IndexedEdge) t;
        return index - other.index;
    }
}
