package backtype.storm.serialization;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.CRC32OutputStream;
import backtype.storm.utils.ThreadResourceManager;
import backtype.storm.utils.WritableUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    ThreadResourceManager<Worker> _manager;
    
    public KryoTupleSerializer(final Map conf, final TopologyContext context) {
        _manager = new ThreadResourceManager<Worker>(new ThreadResourceManager.ResourceFactory<Worker>() {
            @Override
            public Worker makeResource() {
                return new Worker(conf, context);
            }           
        });
    }

    @Override
    public byte[] serialize(Tuple tuple) {
        Worker worker = _manager.acquire();        
        try {
            return worker.serialize(tuple);
        } finally {
            _manager.release(worker);
        }
    }

    @Override
    public long crc32(Tuple tuple) {
        Worker worker = _manager.acquire();        
        try {
            return worker.crc32(tuple);
        } finally {
            _manager.release(worker);
        }
    }
    
    private static class Worker implements ITupleSerializer {
        ByteArrayOutputStream _outputter;
        DataOutputStream _dataOutputter;
        KryoValuesSerializer _kryo;
        SerializationFactory.IdDictionary _ids;

        public Worker(Map conf, TopologyContext context) {
            _outputter = new ByteArrayOutputStream();
            _dataOutputter = new DataOutputStream(_outputter);
            _kryo = new KryoValuesSerializer(conf);
            _ids = new SerializationFactory.IdDictionary(context.getRawTopology());

        }

        public byte[] serialize(Tuple tuple) {
            try {
                _outputter.reset();
                WritableUtils.writeVInt(_dataOutputter, tuple.getSourceTask());
                WritableUtils.writeVInt(_dataOutputter, _ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()));
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
}
