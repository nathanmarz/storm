package backtype.storm.tuple;

import backtype.storm.task.TopologyContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tuple {
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

    public int size() {
        return values.size();
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
    
    @Deprecated
    public List<Object> getTuple() {
        return values;
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
    
    public int getSourceComponent() {
        return context.getComponentId(taskId);
    }
    
    public int getSourceTask() {
        return taskId;
    }
    
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
}
