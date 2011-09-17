package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;
import backtype.storm.utils.CRC32OutputStream;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TupleSerializer {
    ByteArrayOutputStream _outputter;
    DataOutputStream _dataOutputter;
    Map<Integer, Map<Integer, ValuesSerializer>> _fieldSerializers = new HashMap<Integer, Map<Integer, ValuesSerializer>>();
    Map _conf;
    
    public TupleSerializer(Map conf) {
        _outputter = new ByteArrayOutputStream();
        _dataOutputter = new DataOutputStream(_outputter);
        _conf = conf;
    }    
    
    public byte[] serialize(Tuple tuple) throws IOException {
        _outputter.reset();
        WritableUtils.writeVInt(_dataOutputter, tuple.getSourceTask());
        WritableUtils.writeVInt(_dataOutputter, tuple.getSourceStreamId());
        tuple.getMessageId().serialize(_dataOutputter);
        ValuesSerializer streamSerializers = getValuesSerializer(_fieldSerializers, tuple);
        streamSerializers.serializeInto(tuple.getValues(), _dataOutputter);
        return _outputter.toByteArray();
    }

    public long crc32(Tuple tuple) {
        CRC32OutputStream hasher = new CRC32OutputStream();
        try {
            getValuesSerializer(_fieldSerializers, tuple).serializeInto(tuple.getValues(), new DataOutputStream(hasher));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hasher.getValue();
    }    
    
    private ValuesSerializer getValuesSerializer(Map<Integer, Map<Integer, ValuesSerializer>> serializers, Tuple tuple) {
        Map<Integer, ValuesSerializer> streamToSerializers = serializers.get(tuple.getSourceComponent());
        if(streamToSerializers==null) {
            streamToSerializers = new HashMap<Integer, ValuesSerializer>();
            serializers.put(tuple.getSourceComponent(), streamToSerializers);
        }
        ValuesSerializer streamSerializers = streamToSerializers.get(tuple.getSourceStreamId());
        if(streamSerializers==null) {
            streamSerializers = new ValuesSerializer(_conf);
            streamToSerializers.put(tuple.getSourceStreamId(), streamSerializers);
        }
        return streamSerializers;
    }        
}
