package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.ThreadResourceManager;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    ThreadResourceManager<Worker> _manager;
    
    public KryoTupleDeserializer(final Map conf, final TopologyContext context) {
        _manager = new ThreadResourceManager<Worker>(new ThreadResourceManager.ResourceFactory<Worker>() {
            @Override
            public Worker makeResource() {
                return new Worker(conf, context);
            }           
        });
    }
    
    @Override
    public Tuple deserialize(byte[] ser) {
        Worker worker = _manager.acquire();        
        try {
            return worker.deserialize(ser);
        } finally {
            _manager.release(worker);
        }
    }
    
    public static class Worker implements ITupleDeserializer {
        TopologyContext _context;
        KryoValuesDeserializer _kryo;
        SerializationFactory.IdDictionary _ids;
    
        public Worker(Map conf, TopologyContext context) {
            _kryo = new KryoValuesDeserializer(conf);
            _context = context;
            _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        }

        public Tuple deserialize(byte[] ser) {
            try {
                ByteArrayInputStream bin = new ByteArrayInputStream(ser);
                DataInputStream in = new DataInputStream(bin);
                int taskId = WritableUtils.readVInt(in);
                int streamId = WritableUtils.readVInt(in);
                String componentName = _context.getComponentId(taskId);
                String streamName = _ids.getStreamName(componentName, streamId);
                MessageId id = MessageId.deserialize(in);
                List<Object> values = _kryo.deserializeFrom(bin);
                return new Tuple(_context, values, taskId, streamName, id);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
