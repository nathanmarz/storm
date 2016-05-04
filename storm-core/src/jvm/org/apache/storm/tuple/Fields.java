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
package org.apache.storm.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

/**
 * Collection of unique named fields using in an ITuple
 */
public class Fields implements Iterable<String>, Serializable {
    private List<String> _fields;
    private Map<String, Integer> _index = new HashMap<>();
    
    public Fields(String... fields) {
        this(Arrays.asList(fields));
    }
    
    public Fields(List<String> fields) {
        _fields = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (_fields.contains(field))
                throw new IllegalArgumentException(
                    String.format("duplicate field '%s'", field)
                );
            _fields.add(field);
        }
        index();
    }
    
    public List<Object> select(Fields selector, List<Object> tuple) {
        List<Object> ret = new ArrayList<>(selector.size());
        for(String s: selector) {
            ret.add(tuple.get(_index.get(s)));
        }
        return ret;
    }

    public List<String> toList() {
        return new ArrayList<>(_fields);
    }
    
    /**
     * Returns the number of fields in this collection.
     */
    public int size() {
        return _fields.size();
    }

    /**
     * Gets the field at position index in the collection. 
     *  
     * @param index index of the field to return 
     *  
     * @throws IndexOutOfBoundsException - if the index is out of range (index < 0 || index >= size()) 
     */
    public String get(int index) {
        return _fields.get(index);
    }

    public Iterator<String> iterator() {
        return _fields.iterator();
    }
    
    /**
     * Returns the position of the specified named field.
     *  
     * @param field Named field to evaluate
     *  
     * @throws IllegalArgumentException - if field does not exist
     */
    public int fieldIndex(String field) {
        Integer ret = _index.get(field);
        if(ret==null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }
    
    /**
     * @return true if this contains the specified name of the field.
     */
    public boolean contains(String field) {
        return _index.containsKey(field);
    }
    
    private void index() {
        for(int i=0; i<_fields.size(); i++) {
            _index.put(_fields.get(i), i);
        }
    }

    @Override
    public String toString() {
        return _fields.toString();
    }    
}
