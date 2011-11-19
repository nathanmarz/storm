package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;
import backtype.storm.utils.CRC32OutputStream;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    ByteArrayOutputStream _outputter;
    DataOutputStream _dataOutputter;
    KryoValuesSerializer _kryo;
    
    public KryoTupleSerializer(Map conf) {
        _outputter = new ByteArrayOutputStream();
        _dataOutputter = new DataOutputStream(_outputter);
        _kryo = new KryoValuesSerializer(conf);
    }
    
    public byte[] serialize(Tuple tuple) {
        try {
            _outputter.reset();
            WritableUtils.writeVInt(_dataOutputter, tuple.getSourceTask());
            WritableUtils.writeVInt(_dataOutputter, tuple.getSourceStreamId());
            tuple.getMessageId().serialize(_dataOutputter);
            _kryo.serializeInto(tuple.getValues(), _outputter);
            return _outputter.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long crc32(Tuple tuple) {
        try {
            CRC32OutputStream hasher = new CRC32OutputStream();
            _kryo.serializeInto(tuple.getValues(), hasher);
            return hasher.getValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    
}
