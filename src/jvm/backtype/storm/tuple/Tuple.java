package backtype.storm.tuple;

import backtype.storm.task.TopologyContext;
import clojure.lang.ILookup;
import clojure.lang.Keyword;
import clojure.lang.Symbol;
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
public class Tuple implements ILookup {
    private List<Object> values;
    private int taskId;
    private int streamId;
    private TopologyContext context;
    private MessageId id;

    //needs to get taskId explicitly b/c could be in a different task than where it was created
    public Tuple(TopologyContext context, List<Object> values, int taskId, int streamId, MessageId id) {
        this.values = values;
        this.taskId = taskId;
        this.streamId = streamId;
        this.id = id;
        this.context = context;
        //TODO: should find a way to include this information here
        //TODO: should only leave out the connection info?
        //TODO: have separate methods for "user" and "system" topology?
        if(streamId>=0) {
            int componentId = context.getComponentId(taskId);
            if(componentId>=0) {
                Fields schema = context.getComponentOutputFields(componentId, streamId);
                if(values.size()!=schema.size()) {
                    throw new IllegalArgumentException(
                            "Tuple created with wrong number of fields. " +
                            "Expected " + schema.size() + " fields but got " +
                            values.size() + " fields");
                }
            }
        }
    }

    public Tuple(TopologyContext context, List<Object> values, int taskId, int streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }

    public Tuple copyWithNewId(long id) {
        Map<Long, Long> newIds = new HashMap<Long, Long>();
        for(Long anchor: this.id.getAnchorsToIds().keySet()) {
            newIds.put(anchor, id);
        }
        return new Tuple(this.context, this.values, this.taskId, this.streamId, MessageId.makeId(newIds));
    }

    /**
     * Returns the number of fields in this tuple.
     */
    public int size() {
        return values.size();
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
     * Gets the id of the component that created this tuple.
     */
    public int getSourceComponent() {
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
    public int getSourceStreamId() {
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

    private static final Keyword makeKeyword(String name) {
        return Keyword.intern(Symbol.create(name));
    }
    
    private static final Keyword STREAM_KEYWORD = makeKeyword("stream");
    private static final Keyword COMPONENT_KEYWORD = makeKeyword("component");
    private static final Keyword TASK_KEYWORD = makeKeyword("task");
    
    @Override
    public Object valAt(Object o) {
        if(o.equals(STREAM_KEYWORD)) {
            return getSourceStreamId();
        } else if(o.equals(COMPONENT_KEYWORD)) {
            return getSourceComponent();
        } else if(o.equals(TASK_KEYWORD)) {
            return getSourceTask();
        }
        return null;
    }

    @Override
    public Object valAt(Object o, Object def) {
        Object ret = valAt(o);
        if(ret==null) ret = def;
        return ret;
    }
}
