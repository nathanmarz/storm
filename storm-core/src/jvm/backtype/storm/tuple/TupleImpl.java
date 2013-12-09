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
package backtype.storm.tuple;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.utils.IndifferentAccessMap;
import clojure.lang.ASeq;
import clojure.lang.Counted;
import clojure.lang.IMeta;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import clojure.lang.Keyword;
import clojure.lang.MapEntry;
import clojure.lang.Obj;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Seqable;
import clojure.lang.Symbol;
import java.util.List;

public class TupleImpl extends IndifferentAccessMap implements Seqable, Indexed, IMeta, Tuple {
    private List<Object> values;
    private int taskId;
    private String streamId;
    private GeneralTopologyContext context;
    private MessageId id;
    private IPersistentMap _meta = null;
    
    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
        this.values = values;
        this.taskId = taskId;
        this.streamId = streamId;
        this.id = id;
        this.context = context;
        
        String componentId = context.getComponentId(taskId);
        Fields schema = context.getComponentOutputFields(componentId, streamId);
        if(values.size()!=schema.size()) {
            throw new IllegalArgumentException(
                    "Tuple created with wrong number of fields. " +
                    "Expected " + schema.size() + " fields but got " +
                    values.size() + " fields");
        }
    }

    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }    
    
    Long _processSampleStartTime = null;
    Long _executeSampleStartTime = null;
    
    public void setProcessSampleStartTime(long ms) {
        _processSampleStartTime = ms;
    }

    public Long getProcessSampleStartTime() {
        return _processSampleStartTime;
    }
    
    public void setExecuteSampleStartTime(long ms) {
        _executeSampleStartTime = ms;
    }

    public Long getExecuteSampleStartTime() {
        return _executeSampleStartTime;
    }
    
    long _outAckVal = 0;
    
    public void updateAckVal(long val) {
        _outAckVal = _outAckVal ^ val;
    }
    
    public long getAckVal() {
        return _outAckVal;
    }

    public int size() {
        return values.size();
    }
    
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }
    
    public boolean contains(String field) {
        return getFields().contains(field);
    }
    
    public Object getValue(int i) {
        return values.get(i);
    }

    public String getString(int i) {
        return (String) values.get(i);
    }

    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }
    
    
    public Object getValueByField(String field) {
        return values.get(fieldIndex(field));
    }

    public String getStringByField(String field) {
        return (String) values.get(fieldIndex(field));
    }

    public Integer getIntegerByField(String field) {
        return (Integer) values.get(fieldIndex(field));
    }

    public Long getLongByField(String field) {
        return (Long) values.get(fieldIndex(field));
    }

    public Boolean getBooleanByField(String field) {
        return (Boolean) values.get(fieldIndex(field));
    }

    public Short getShortByField(String field) {
        return (Short) values.get(fieldIndex(field));
    }

    public Byte getByteByField(String field) {
        return (Byte) values.get(fieldIndex(field));
    }

    public Double getDoubleByField(String field) {
        return (Double) values.get(fieldIndex(field));
    }

    public Float getFloatByField(String field) {
        return (Float) values.get(fieldIndex(field));
    }

    public byte[] getBinaryByField(String field) {
        return (byte[]) values.get(fieldIndex(field));
    }
    
    public List<Object> getValues() {
        return values;
    }
    
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }

    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
    }
      
    public GlobalStreamId getSourceGlobalStreamid() {
        return new GlobalStreamId(getSourceComponent(), streamId);
    }
    
    public String getSourceComponent() {
        return context.getComponentId(taskId);
    }
    
    public int getSourceTask() {
        return taskId;
    }
    
    public String getSourceStreamId() {
        return streamId;
    }
    
    public MessageId getMessageId() {
        return id;
    }
    
    @Override
    public String toString() {
        return "source: " + getSourceComponent() + ":" + taskId + ", stream: " + streamId + ", id: "+ id.toString() + ", " + values.toString();
    }
    
    @Override
    public boolean equals(Object other) {
        return this == other;
    }    
    
    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    private final Keyword makeKeyword(String name) {
        return Keyword.intern(Symbol.create(name));
    }    

    /* ILookup */
    @Override
    public Object valAt(Object o) {
        try {
            if(o instanceof Keyword) {
                return getValueByField(((Keyword) o).getName());
            } else if(o instanceof String) {
                return getValueByField((String) o);
            }
        } catch(IllegalArgumentException e) {
        }
        return null;
    }

    /* Seqable */
    public ISeq seq() {
        if(values.size() > 0) {
            return new Seq(getFields().toList(), values, 0);
        }
        return null;
    }

    static class Seq extends ASeq implements Counted {
        final List<String> fields;
        final List<Object> values;
        final int i;

        Seq(List<String> fields, List<Object> values, int i) {
            this.fields = fields;
            this.values = values;
            assert i >= 0;
            this.i = i;
        }

        public Seq(IPersistentMap meta, List<String> fields, List<Object> values, int i) {
            super(meta);
            this.fields= fields;
            this.values = values;
            assert i >= 0;
            this.i = i;
        }

        public Object first() {
            return new MapEntry(fields.get(i), values.get(i));
        }

        public ISeq next() {
            if(i+1 < fields.size()) {
                return new Seq(fields, values, i+1);
            }
            return null;
        }

        public int count() {
            assert fields.size() -i >= 0 : "index out of bounds";
            // i being the position in the fields of this seq, the remainder of the seq is the size
            return fields.size() -i;
        }

        public Obj withMeta(IPersistentMap meta) {
            return new Seq(meta, fields, values, i);
        }
    }

    /* Indexed */
    public Object nth(int i) {
        if(i < values.size()) {
            return values.get(i);
        } else {
            return null;
        }
    }

    public Object nth(int i, Object notfound) {
        Object ret = nth(i);
        if(ret==null) ret = notfound;
        return ret;
    }

    /* Counted */
    public int count() {
        return values.size();
    }
    
    /* IMeta */
    public IPersistentMap meta() {
        if(_meta==null) {
            _meta = new PersistentArrayMap( new Object[] {
            makeKeyword("stream"), getSourceStreamId(), 
            makeKeyword("component"), getSourceComponent(), 
            makeKeyword("task"), getSourceTask()});
        }
        return _meta;
    }

    private PersistentArrayMap toMap() {
        Object array[] = new Object[values.size()*2];
        List<String> fields = getFields().toList();
        for(int i=0; i < values.size(); i++) {
            array[i*2] = fields.get(i);
            array[(i*2)+1] = values.get(i);
        }
        return new PersistentArrayMap(array);
    }

    public IPersistentMap getMap() {
        if(_map==null) {
            setMap(toMap());
        }
        return _map;
    }    
    
}
