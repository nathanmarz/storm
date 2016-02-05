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
package org.apache.storm.trident.tuple;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

/**
 * Extends AbstractList so that it can be emitted directly as Storm tuples
 */
public class TridentTupleView extends AbstractList<Object> implements TridentTuple {
    ValuePointer[] _index;
    Map<String, ValuePointer> _fieldIndex;
    IPersistentVector _delegates;

    public static class ProjectionFactory implements Factory {
        Map<String, ValuePointer> _fieldIndex;
        ValuePointer[] _index;
        Factory _parent;

        public ProjectionFactory(Factory parent, Fields projectFields) {
            _parent = parent;
            if(projectFields==null) projectFields = new Fields();
            Map<String, ValuePointer> parentFieldIndex = parent.getFieldIndex();
            _fieldIndex = new HashMap<>();
            for(String f: projectFields) {
                _fieldIndex.put(f, parentFieldIndex.get(f));
            }            
            _index = ValuePointer.buildIndex(projectFields, _fieldIndex);
        }
        
        public TridentTuple create(TridentTuple parent) {
            if(_index.length==0) return EMPTY_TUPLE;
            else return new TridentTupleView(((TridentTupleView)parent)._delegates, _index, _fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return _fieldIndex;
        }

        @Override
        public int numDelegates() {
            return _parent.numDelegates();
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(_index);
        }
    }
    
    public static class FreshOutputFactory  implements Factory {
        Map<String, ValuePointer> _fieldIndex;
        ValuePointer[] _index;

        public FreshOutputFactory(Fields selfFields) {
            _fieldIndex = new HashMap<>();
            for(int i=0; i<selfFields.size(); i++) {
                String field = selfFields.get(i);
                _fieldIndex.put(field, new ValuePointer(0, i, field));
            }
            _index = ValuePointer.buildIndex(selfFields, _fieldIndex);
        }
        
        public TridentTuple create(List<Object> selfVals) {
            return new TridentTupleView(PersistentVector.EMPTY.cons(selfVals), _index, _fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return _fieldIndex;
        }

        @Override
        public int numDelegates() {
            return 1;
        }
        
        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(_index);
        }        
    }
    
    public static class OperationOutputFactory implements Factory {
        Map<String, ValuePointer> _fieldIndex;
        ValuePointer[] _index;
        Factory _parent;

        public OperationOutputFactory(Factory parent, Fields selfFields) {
            _parent = parent;
            _fieldIndex = new HashMap<>(parent.getFieldIndex());
            int myIndex = parent.numDelegates();
            for(int i=0; i<selfFields.size(); i++) {
                String field = selfFields.get(i);
                _fieldIndex.put(field, new ValuePointer(myIndex, i, field));
            }
            List<String> myOrder = new ArrayList<>(parent.getOutputFields());
            
            Set<String> parentFieldsSet = new HashSet<>(myOrder);
            for(String f: selfFields) {
                if(parentFieldsSet.contains(f)) {
                    throw new IllegalArgumentException(
                            "Additive operations cannot add fields with same name as already exists. "
                            + "Tried adding " + selfFields + " to " + parent.getOutputFields());
                }
                myOrder.add(f);
            }
            
            _index = ValuePointer.buildIndex(new Fields(myOrder), _fieldIndex);
        }
        
        public TridentTuple create(TridentTupleView parent, List<Object> selfVals) {
            IPersistentVector curr = parent._delegates;
            curr = (IPersistentVector) RT.conj(curr, selfVals);
            return new TridentTupleView(curr, _index, _fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return _fieldIndex;
        }

        @Override
        public int numDelegates() {
            return _parent.numDelegates() + 1;
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(_index);
        }
    }
    
    public static class RootFactory implements Factory {
        ValuePointer[] index;
        Map<String, ValuePointer> fieldIndex;
        
        public RootFactory(Fields inputFields) {
            index = new ValuePointer[inputFields.size()];
            int i=0;
            for(String f: inputFields) {
                index[i] = new ValuePointer(0, i, f);
                i++;
            }
            fieldIndex = ValuePointer.buildFieldIndex(index);
        }
        
        public TridentTuple create(Tuple parent) {            
            return new TridentTupleView(PersistentVector.EMPTY.cons(parent.getValues()), index, fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return fieldIndex;
        }

        @Override
        public int numDelegates() {
            return 1;
        }
        
        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(this.index);
        }
    }
    
    private static List<String> indexToFieldsList(ValuePointer[] index) {
        List<String> ret = new ArrayList<>();
        for(ValuePointer p: index) {
            ret.add(p.field);
        }
        return ret;
    }
    
    public static final TridentTupleView EMPTY_TUPLE = new TridentTupleView(null, new ValuePointer[0], new HashMap());

    // index and fieldIndex are precomputed, delegates built up over many operations using persistent data structures
    public TridentTupleView(IPersistentVector delegates, ValuePointer[] index, Map<String, ValuePointer> fieldIndex) {
        _delegates = delegates;
        _index = index;
        _fieldIndex = fieldIndex;
    }

    public static TridentTuple createFreshTuple(Fields fields, List<Object> values) {
        FreshOutputFactory factory = new FreshOutputFactory(fields);
        return factory.create(values);
    }

    public static TridentTuple createFreshTuple(Fields fields, Object... values) {
        FreshOutputFactory factory = new FreshOutputFactory(fields);
        return factory.create(Arrays.asList(values));
    }

    @Override
    public List<Object> getValues() {
        return this;
    }    

    @Override
    public int size() {
        return _index.length;
    }

    @Override
    public boolean contains(String field) {
        return getFields().contains(field);
    }

    @Override
    public Fields getFields() {
        return new Fields(indexToFieldsList(_index));
    }

    @Override
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        return getFields().select(selector, getValues());
    }

    @Override
    public Object get(int i) {
        return getValue(i);
    }    
    
    @Override
    public Object getValue(int i) {
        return getValueByPointer(_index[i]);
    }

    @Override
    public String getString(int i) {
        return (String) getValue(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) getValue(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) getValue(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) getValue(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) getValue(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) getValue(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) getValue(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) getValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) getValue(i);
    }

    @Override
    public Object getValueByField(String field) {
        return getValueByPointer(_fieldIndex.get(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) getValueByField(field);
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) getValueByField(field);
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) getValueByField(field);
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) getValueByField(field);
    }

    @Override
    public Short getShortByField(String field) {
        return (Short) getValueByField(field);
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) getValueByField(field);
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) getValueByField(field);
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) getValueByField(field);
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) getValueByField(field);
    }

    private Object getValueByPointer(ValuePointer ptr) {
        return ((List<Object>)_delegates.nth(ptr.delegateIndex)).get(ptr.index);     
    }
}
