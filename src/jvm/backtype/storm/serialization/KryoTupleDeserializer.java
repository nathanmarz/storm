package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    TopologyContext _context;
    KryoValuesDeserializer _kryo;
    
    public KryoTupleDeserializer(Map conf, TopologyContext context) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
    }

    public Tuple deserialize(byte[] ser) {
        try {
            ByteArrayInputStream bin = new ByteArrayInputStream(ser);
            DataInputStream in = new DataInputStream(bin);
            int taskId = WritableUtils.readVInt(in);
            int streamId = WritableUtils.readVInt(in);
            MessageId id = MessageId.deserialize(in);
            List<Object> values = _kryo.deserializeFrom(bin);
            return new Tuple(_context, values, taskId, streamId, id);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }    
}
