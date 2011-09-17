package backtype.storm.tuple;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class TupleImpl implements Tuple {
    private List<Object> values;
    private List<Object> metadata;
    private int taskId;
    private List<Integer> streamId;
    private TopologyContext context;
    private MessageId id;


    //needs to get taskId explicitly b/c could be in a different task than where it was created
    public TupleImpl(TopologyContext context, List<Object> values, int taskId, List<Integer> streamId, List<Object> metadata, MessageId id) {
        if(streamId.size()==0) {
            throw new IllegalArgumentException("Cannot have an empty stream id");
        }
        this.values = values;
        this.taskId = taskId;
        this.streamId = streamId;
        this.id = id;
        this.context = context;
        
        int majorStream = streamId.get(0);
        if(majorStream>=0) {
            int componentId = context.getComponentId(taskId);
            if(componentId>=0) {
                Fields schema = context.getComponentOutputFields(componentId, majorStream);
                if(values.size()!=schema.size()) {
                    throw new IllegalArgumentException(
                            "Tuple created with wrong number of fields. " +
                            "Expected " + schema.size() + " fields but got " +
                            values.size() + " fields");
                }
            }
        }        
//        this doesn't work because of implicit streams that don't exist in context (ackers)
//        int expectedSize = context.getComponentOutputFields(getSourceComponent(), streamId).size();
//        if(expectedSize!=values.size()) {
//            throw new RuntimeException("Created tuple with invalid number of fields: " + values.toString());
//        }
    }

    public TupleImpl(TopologyContext context, List<Object> values, int taskId, List<Integer> streamId, MessageId id) {
        this(context, values, taskId, streamId, null, id);
    }

    public TupleImpl(TopologyContext context, List<Object> values, int taskId, List<Integer> streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }

    public TupleImpl(TopologyContext context, List<Object> values, int taskId, List<Integer> streamId, List<Object> metadata) {
        this(context, values, taskId, streamId, metadata, MessageId.makeUnanchored());
    }

    public List<Object> getMetadata() {
        return metadata;
    }

    public Object getMetadataValue(int i) {
        return metadata.get(i);
    }

    public int metadataSize() {
        return metadata.size();
    }

    public Tuple copyWithNewId(long id) {
        Map<Long, Long> newIds = new HashMap<Long, Long>();
        for(Long anchor: this.id.getAnchorsToIds().keySet()) {
            newIds.put(anchor, id);
        }
        return new TupleImpl(this.context, this.values, this.taskId, this.streamId, metadata, MessageId.makeId(newIds));
    }

    public int size() {
        return values.size();
    }

    public Object getValue(int i) {
        return values.get(i);
    }
    
    @Deprecated
    public List<Object> getTuple() {
        return values;
    }

    public List<Object> getValues() {
        return values;
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
    
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }

    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
    }
    
    public int getSourceComponent() {
        return context.getComponentId(taskId);
    }
    
    public int getSourceTask() {
        return taskId;
    }
    
    public int getSourceStreamId() {
        return streamId.get(0);
    }
    
    public MessageId getMessageId() {
        return id;
    }
    
    public boolean isFromFailureStream() {
        return streamId.size()==2 &&
                context.isSpout(streamId.get(0)) &&
                streamId.get(1).equals(Constants.FAILURE_SUBSTREAM);
    }
    
    public List<Integer> getFullStreamId() {
        return streamId;
    }
    
    @Override
    public int hashCode() {
        // for OutputCollector
        return System.identityHashCode(this);
    }
        
    @Override
    public boolean equals(Object other) {
        // for OutputCollector
        return this == other;
    }
    
    @Override
    public String toString() {
        return "source: " + getSourceComponent() + ":" + taskId + ", stream: " + streamId + ", id: "+ id.toString() + ", " + values.toString();
    }
}