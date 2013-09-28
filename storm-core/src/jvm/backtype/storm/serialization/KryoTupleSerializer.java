package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    KryoValuesSerializer _kryo;
    SerializationFactory.IdDictionary _ids;   
    Output _kryoOut;
    
    public KryoTupleSerializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
    }

    public byte[] serialize(Tuple tuple) {
        try {
            
            _kryoOut.clear();
            _kryoOut.writeInt(tuple.getSourceTask(), true);
            _kryoOut.writeInt(_ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(_kryoOut);
            _kryo.serializeInto(tuple.getValues(), _kryoOut);
            return _kryoOut.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public long crc32(Tuple tuple) {
//        try {
//            CRC32OutputStream hasher = new CRC32OutputStream();
//            _kryo.serializeInto(tuple.getValues(), hasher);
//            return hasher.getValue();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
