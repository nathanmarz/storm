package storm.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


public class TransactionalKafkaSpout extends BasePartitionedTransactionalSpout<BatchMeta> {
    public static final String ATTEMPT_FIELD = TransactionalKafkaSpout.class.getCanonicalName() + "/attempt";
    
    KafkaConfig _config;
    
    public TransactionalKafkaSpout(KafkaConfig config) {
        _config = config;
    }
    
    class Coordinator implements IPartitionedTransactionalSpout.Coordinator {
        @Override
        public int numPartitions() {
            return computeNumPartitions();
        }

        @Override
        public void close() {
        }
    }
    
    class Emitter implements IPartitionedTransactionalSpout.Emitter<BatchMeta> {
        KafkaPartitionConnections _connections;
        
        public Emitter() {
            _connections = new KafkaPartitionConnections(_config);
        }
        
        @Override
        public BatchMeta emitPartitionBatchNew(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta lastMeta) {
            SimpleConsumer consumer = _connections.getConsumer(partition);

            return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta);
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta meta) {
            SimpleConsumer consumer = _connections.getConsumer(partition);
                        
            ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, partition % _config.partitionsPerHost, meta.offset, _config.fetchSizeBytes));
            long offset = meta.offset;
            for(MessageAndOffset msg: msgs) {
                if(offset == meta.nextOffset) break;
                if(offset > meta.nextOffset) {
                    throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                }
                KafkaUtils.emit(_config, attempt, collector, msg.message());
                offset = msg.offset();
            }            
        }
        
        @Override
        public void close() {
            _connections.close();
        }
    }
    

    @Override
    public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTransactionalSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<String>(_config.scheme.getOutputFields().toList());
        fields.add(0, ATTEMPT_FIELD);
        declarer.declare(new Fields(fields));
    }
    
    private int computeNumPartitions() {
        return _config.hosts.size() * _config.partitionsPerHost;        
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
}