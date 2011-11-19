package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;
import backtype.storm.utils.CRC32OutputStream;
import backtype.storm.utils.ListDelegate;
import backtype.storm.utils.WritableUtils;
import com.esotericsoftware.kryo.ObjectBuffer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    ByteArrayOutputStream _outputter;
    DataOutputStream _dataOutputter;
    ObjectBuffer _kryo;
    ListDelegate _delegate;
    
    public KryoTupleSerializer(Map conf) {
        _outputter = new ByteArrayOutputStream();
        _dataOutputter = new DataOutputStream(_outputter);
        _kryo = KryoFactory.getKryo(conf);
        _delegate = new ListDelegate();
    }
    
    public byte[] serialize(Tuple tuple) throws IOException {
        _outputter.reset();
        WritableUtils.writeVInt(_dataOutputter, tuple.getSourceTask());
        WritableUtils.writeVInt(_dataOutputter, tuple.getSourceStreamId());
        tuple.getMessageId().serialize(_dataOutputter);
        // this ensures that list of values is always written the same way, regardless
        // of whether it's a java collection or one of clojure's persistent collections 
        // (which have different serializers)
        // Doing this lets us deserialize as ArrayList and avoid writing the class here
        _delegate.setDelegate(tuple.getValues());
        _kryo.writeObject(_outputter, _delegate);
        return _outputter.toByteArray();
    }

    public long crc32(Tuple tuple) {
        CRC32OutputStream hasher = new CRC32OutputStream();
        _kryo.writeObject(hasher, tuple.getValues());
        return hasher.getValue();       
    }
    
    
}
