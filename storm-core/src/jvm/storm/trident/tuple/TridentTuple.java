package storm.trident.tuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface TridentTuple extends List<Object> {
    public static interface Factory extends Serializable {
        Map<String, ValuePointer> getFieldIndex();
        List<String> getOutputFields();
        int numDelegates();
    }

    List<Object> getValues();
    
    Object getValue(int i);
    
    String getString(int i);
    
    Integer getInteger(int i);
    
    Long getLong(int i);
    
    Boolean getBoolean(int i);
    
    Short getShort(int i);
    
    Byte getByte(int i);
    
    Double getDouble(int i);
    
    Float getFloat(int i);
    
    byte[] getBinary(int i);    
    
    Object getValueByField(String field);
    
    String getStringByField(String field);
    
    Integer getIntegerByField(String field);
    
    Long getLongByField(String field);
    
    Boolean getBooleanByField(String field);
    
    Short getShortByField(String field);
    
    Byte getByteByField(String field);
    
    Double getDoubleByField(String field);
    
    Float getFloatByField(String field);
    
    byte[] getBinaryByField(String field);
}
