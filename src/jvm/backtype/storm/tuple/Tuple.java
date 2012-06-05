package backtype.storm.tuple;

import backtype.storm.generated.GlobalStreamId;
import java.util.List;

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
public interface Tuple {

    /**
     * Returns the number of fields in this tuple.
     */
    public int size();
    
    public int fieldIndex(String field);
    
    /**
     * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
     */
    public Object getValue(int i);

    /**
     * Returns the String at position i in the tuple. If that field is not a String, 
     * you will get a runtime error.
     */
    public String getString(int i);

    /**
     * Returns the Integer at position i in the tuple. If that field is not an Integer, 
     * you will get a runtime error.
     */
    public Integer getInteger(int i);

    /**
     * Returns the Long at position i in the tuple. If that field is not a Long, 
     * you will get a runtime error.
     */
    public Long getLong(int i);

    /**
     * Returns the Boolean at position i in the tuple. If that field is not a Boolean, 
     * you will get a runtime error.
     */
    public Boolean getBoolean(int i);

    /**
     * Returns the Short at position i in the tuple. If that field is not a Short, 
     * you will get a runtime error.
     */
    public Short getShort(int i);

    /**
     * Returns the Byte at position i in the tuple. If that field is not a Byte, 
     * you will get a runtime error.
     */
    public Byte getByte(int i);

    /**
     * Returns the Double at position i in the tuple. If that field is not a Double, 
     * you will get a runtime error.
     */
    public Double getDouble(int i);

    /**
     * Returns the Float at position i in the tuple. If that field is not a Float, 
     * you will get a runtime error.
     */
    public Float getFloat(int i);

    /**
     * Returns the byte array at position i in the tuple. If that field is not a byte array, 
     * you will get a runtime error.
     */
    public byte[] getBinary(int i);
    
    
    public Object getValueByField(String field);

    public String getStringByField(String field);

    public Integer getIntegerByField(String field);

    public Long getLongByField(String field);

    public Boolean getBooleanByField(String field);

    public Short getShortByField(String field);

    public Byte getByteByField(String field);

    public Double getDoubleByField(String field);

    public Float getFloatByField(String field);

    public byte[] getBinaryByField(String field);
    
    /**
     * Gets all the values in this tuple.
     */
    public List<Object> getValues();
    
    /**
     * Gets the names of the fields in this tuple.
     */
    public Fields getFields();

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    public List<Object> select(Fields selector);
    
    
    /**
     * Returns the global stream id (component + stream) of this tuple.
     */
    public GlobalStreamId getSourceGlobalStreamid();
    
    /**
     * Gets the id of the component that created this tuple.
     */
    public String getSourceComponent();
    
    /**
     * Gets the id of the task that created this tuple.
     */
    public int getSourceTask();
    
    /**
     * Gets the id of the stream that this tuple was emitted to.
     */
    public String getSourceStreamId();
    
    public MessageId getMessageId();
}
