package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.WritableUtils;
import com.esotericsoftware.kryo.ObjectBuffer;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    ObjectBuffer _kryo;
    TopologyContext _context;
    
    public KryoTupleDeserializer(Map conf, TopologyContext context) {
        _kryo = KryoFactory.getKryo(conf);
        _context = context;
    }

    public Tuple deserialize(byte[] ser) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(ser);
        DataInputStream in = new DataInputStream(bin);
        int taskId = WritableUtils.readVInt(in);
        int streamId = WritableUtils.readVInt(in);
        MessageId id = MessageId.deserialize(in);
        List<Object> values = (List<Object>) _kryo.readObject(in, ArrayList.class);
        return new Tuple(_context, values, taskId, streamId, id);
    }    
}
