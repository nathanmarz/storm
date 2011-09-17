package backtype.storm.serialization;

import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.CRC32OutputStream;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TupleSerializer {
    ByteArrayOutputStream _outputter;
    DataOutputStream _dataOutputter;
    Map<Integer, Map<List<Integer>, ValuesSerializer>> _fieldSerializers = new HashMap<Integer, Map<List<Integer>, ValuesSerializer>>();
    Map<Integer, Map<List<Integer>, ValuesSerializer>> _metaSerializers = new HashMap<Integer, Map<List<Integer>, ValuesSerializer>>();
    Map _conf;
    
    public TupleSerializer(Map conf) {
        _outputter = new ByteArrayOutputStream();
        _dataOutputter = new DataOutputStream(_outputter);
        _conf = conf;
    }    
    
    public byte[] serialize(TupleImpl tuple) throws IOException {
        _outputter.reset();
        WritableUtils.writeVInt(_dataOutputter, tuple.getSourceTask());

        List<Integer> fullStreamId = tuple.getFullStreamId();
        WritableUtils.writeVInt(_dataOutputter, fullStreamId.size());
        for(Integer id: fullStreamId) {
            WritableUtils.writeVInt(_dataOutputter, id);
        }
        tuple.getMessageId().serialize(_dataOutputter);

        ValuesSerializer streamSerializers = getValuesSerializer(_fieldSerializers, tuple);
        streamSerializers.serializeInto(tuple.getTuple(), _dataOutputter);
        ValuesSerializer metaSerializers = getValuesSerializer(_metaSerializers, tuple);
        metaSerializers.serializeInto(tuple.getMetadata(), _dataOutputter);

        return _outputter.toByteArray();
    }

    public long crc32(TupleImpl tuple) {
        CRC32OutputStream hasher = new CRC32OutputStream();
        try {
            getValuesSerializer(_fieldSerializers, tuple).serializeInto(tuple.getValues(), new DataOutputStream(hasher));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hasher.getValue();
    }    
    
    private ValuesSerializer getValuesSerializer(Map<Integer, Map<List<Integer>, ValuesSerializer>> serializers, TupleImpl tuple) {
        Map<List<Integer>, ValuesSerializer> streamToSerializers = serializers.get(tuple.getSourceComponent());
        if(streamToSerializers==null) {
            streamToSerializers = new HashMap<List<Integer>, ValuesSerializer>();
            serializers.put(tuple.getSourceComponent(), streamToSerializers);
        }
        ValuesSerializer streamSerializers = streamToSerializers.get(tuple.getFullStreamId());
        if(streamSerializers==null) {
            streamSerializers = new ValuesSerializer(_conf);
            streamToSerializers.put(tuple.getFullStreamId(), streamSerializers);
        }
        return streamSerializers;
    }        
}
