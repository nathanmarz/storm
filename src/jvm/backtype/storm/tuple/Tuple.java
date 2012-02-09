package backtype.storm.tuple;

import backtype.storm.utils.IndifferentAccessMap;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import clojure.lang.Seqable;
import clojure.lang.Indexed;
import clojure.lang.Counted;
import clojure.lang.ISeq;
import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Obj;
import clojure.lang.IMeta;
import clojure.lang.Keyword;
import clojure.lang.Symbol;
import clojure.lang.MapEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The tuple is the main data structure in Storm. A tuple is a named list of values, 
 * where each value can be any type. Tuples are dynamically typed -- the types of the fields 
 * do not need to be declared. Tuples have helper methods like getInteger and getString 
 * to get field values without having to cast the result.
 * 
 * Storm needs to know how to serialize all the values in a tuple. By default, Storm 
 * knows how to serialize the primitive types, strings, and byte arrays. If you want to 
 * use another type, you'll need to implement and register a serializer for that type.
 * See {@link http://github.com/nathanmarz/storm/wiki/Serialization} for more info.
 */
public class Tuple extends IndifferentAccessMap implements Seqable, Indexed, IMeta {
    private List<Object> values;
    private int taskId;
    private String streamId;
    private TopologyContext context;
    private MessageId id;
    private IPersistentMap _meta = null;

    //needs to get taskId explicitly b/c could be in a different task than where it was created
    public Tuple(TopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
        super();
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

    public Tuple(TopologyContext context, List<Object> values, int taskId, String streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }

    /**
     * Returns the number of fields in this tuple.
     */
    public int size() {
        return values.size();
    }
    
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }
    
    /**
     * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
     */
    public Object getValue(int i) {
        return values.get(i);
    }

    /**
     * Returns the String at position i in the tuple. If that field is not a String, 
     * you will get a runtime error.
     */
    public String getString(int i) {
        return (String) values.get(i);
    }

    /**
     * Returns the Integer at position i in the tuple. If that field is not an Integer, 
     * you will get a runtime error.
     */
    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    /**
     * Returns the Long at position i in the tuple. If that field is not a Long, 
     * you will get a runtime error.
     */
    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    /**
     * Returns the Boolean at position i in the tuple. If that field is not a Boolean, 
     * you will get a runtime error.
     */
    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    /**
     * Returns the Short at position i in the tuple. If that field is not a Short, 
     * you will get a runtime error.
     */
    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    /**
     * Returns the Byte at position i in the tuple. If that field is not a Byte, 
     * you will get a runtime error.
     */
    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    /**
     * Returns the Double at position i in the tuple. If that field is not a Double, 
     * you will get a runtime error.
     */
    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    /**
     * Returns the Float at position i in the tuple. If that field is not a Float, 
     * you will get a runtime error.
     */
    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    /**
     * Returns the byte array at position i in the tuple. If that field is not a byte array, 
     * you will get a runtime error.
     */
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
    
    @Deprecated
    public List<Object> getTuple() {
        return values;
    }

    /**
     * Gets all the values in this tuple.
     */
    public List<Object> getValues() {
        return values;
    }
    
    /**
     * Gets the names of the fields in this tuple.
     */
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
    }
    
    
    /**
     * Returns the global stream id (component + stream) of this tuple.
     */
    public GlobalStreamId getSourceGlobalStreamid() {
        return new GlobalStreamId(getSourceComponent(), streamId);
    }
    
    /**
     * Gets the id of the component that created this tuple.
     */
    public String getSourceComponent() {
        return context.getComponentId(taskId);
    }
    
    /**
     * Gets the id of the task that created this tuple.
     */
    public int getSourceTask() {
        return taskId;
    }
    
    /**
     * Gets the id of the stream that this tuple was emitted to.
     */
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
        // for OutputCollector
        return this == other;
    }
    
    @Override
    public int hashCode() {
        // for OutputCollector
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
            this.i = i;
        }

        public Seq(IPersistentMap meta, List<String> fields, List<Object> values, int i) {
            super(meta);
            this.fields= fields;
            this.values = values;
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
            return fields.size();
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
