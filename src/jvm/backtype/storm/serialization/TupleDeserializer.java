package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleDeserializer {
    Map<Integer, Map<List<Integer>, ValuesDeserializer>> _fieldSerializers = new HashMap<Integer, Map<List<Integer>, ValuesDeserializer>>();
    Map<Integer, Map<List<Integer>, ValuesDeserializer>> _metaSerializers = new HashMap<Integer, Map<List<Integer>, ValuesDeserializer>>();
    Map _conf;
    TopologyContext _context;

    public TupleDeserializer(Map conf, TopologyContext context) {
        _conf = conf;
        _context = context;
    }

    public TupleImpl deserialize(byte[] ser) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(ser);
        DataInputStream in = new DataInputStream(bin);
        int taskId = WritableUtils.readVInt(in);

        int numStreams = WritableUtils.readVInt(in);
        List<Integer> fullStreamId = new ArrayList<Integer>(numStreams);
        for(int i=0; i<numStreams; i++) {
            fullStreamId.add(WritableUtils.readVInt(in));
        }
        MessageId id = MessageId.deserialize(in);
        int componentId = _context.getComponentId(taskId);
        ValuesDeserializer streamSerializers = getValuesDeserializer(_fieldSerializers, componentId, fullStreamId);
        List<Object> values = streamSerializers.deserializeFrom(in);
        ValuesDeserializer metaSerializers = getValuesDeserializer(_metaSerializers, componentId, fullStreamId);
        List<Object> metadata = metaSerializers.deserializeFrom(in);
        return new TupleImpl(_context, values, taskId, fullStreamId, metadata, id);
    }
    
    private ValuesDeserializer getValuesDeserializer(Map<Integer, Map<List<Integer>, ValuesDeserializer>> deserializers, int componentId, List<Integer> fullStreamId) {
        Map<List<Integer>, ValuesDeserializer> streamToSerializers = deserializers.get(componentId);
        if(streamToSerializers==null) {
            streamToSerializers = new HashMap<List<Integer>, ValuesDeserializer>();
            deserializers.put(componentId, streamToSerializers);
        }
        ValuesDeserializer streamSerializers = streamToSerializers.get(fullStreamId);
        if(streamSerializers==null) {
            streamSerializers = new ValuesDeserializer(_conf);
            streamToSerializers.put(fullStreamId, streamSerializers);
        }
        return streamSerializers;        
    }
}