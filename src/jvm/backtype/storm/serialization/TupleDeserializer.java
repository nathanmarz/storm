package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleDeserializer {
    Map<Integer, Map<Integer, ValuesDeserializer>> _fieldSerializers = new HashMap<Integer, Map<Integer, ValuesDeserializer>>();
    Map _conf;
    TopologyContext _context;

    public TupleDeserializer(Map conf, TopologyContext context) {
        _conf = conf;
        _context = context;
    }

    public Tuple deserialize(byte[] ser) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(ser);
        DataInputStream in = new DataInputStream(bin);
        int taskId = WritableUtils.readVInt(in);
        int streamId = WritableUtils.readVInt(in);
        MessageId id = MessageId.deserialize(in);
        int componentId = _context.getComponentId(taskId);
        ValuesDeserializer streamSerializers = getValuesDeserializer(_fieldSerializers, componentId, streamId);
        List<Object> values = streamSerializers.deserializeFrom(in);
        return new Tuple(_context, values, taskId, streamId, id);
    }
    
    private ValuesDeserializer getValuesDeserializer(Map<Integer, Map<Integer, ValuesDeserializer>> deserializers, int componentId, int streamId) {
        Map<Integer, ValuesDeserializer> streamToSerializers = deserializers.get(componentId);
        if(streamToSerializers==null) {
            streamToSerializers = new HashMap<Integer, ValuesDeserializer>();
            deserializers.put(componentId, streamToSerializers);
        }
        ValuesDeserializer streamSerializers = streamToSerializers.get(streamId);
        if(streamSerializers==null) {
            streamSerializers = new ValuesDeserializer(_conf);
            streamToSerializers.put(streamId, streamSerializers);
        }
        return streamSerializers;        
    }
}