package backtype.storm.tuple;

import backtype.storm.task.TopologyContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Tuple {
    List<Object> getTuple();
    Object getValue(int i);    
    int size();
    List<Object> getValues();
    String getString(int i);
    Integer getInteger(int i);
    Long getLong(int i);
    Boolean getBoolean(int i);
    Short getShort(int i);
    Byte getByte(int i);
    Double getDouble(int i);
    Float getFloat(int i);
    byte[] getBinary(int i);
    List<Object> getMetadata();
    Object getMetadataValue(int i);
    int metadataSize();
    MessageId getMessageId();    
    List<Object> select(Fields selector);    
    Fields getFields();
    int getSourceComponent();    
    int getSourceTask();    
    //returning null is the default stream id
    int getSourceStreamId();        
    Tuple copyWithNewId(long id);
    boolean isFromFailureStream();
}
