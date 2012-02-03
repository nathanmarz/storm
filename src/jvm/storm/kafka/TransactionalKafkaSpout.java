package storm.kafka;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;


public class TransactionalKafkaSpout extends BasePartitionedTransactionalSpout<BatchMeta> {
    public static final String ATTEMPT_FIELD = TransactionalKafkaSpout.class.getCanonicalName() + "/attempt";
    
    public static class Config implements Serializable {
        public List<String> hosts;
        public int port = 9092;
        public int partitionsPerHost;
        public int fetchSizeBytes = 1024*1024;
        public int socketTimeoutMs = 10000;
        public int bufferSizeBytes = 1024*1024;
        public Scheme scheme = new RawScheme();
        public String topic;
      
        public Config(List<String> hosts, int partitionsPerHost, String topic) {
            this.hosts = hosts;
            this.partitionsPerHost = partitionsPerHost;
            this.topic = topic;
        }
    }
    
    Config _config;
    
    public TransactionalKafkaSpout(Config config) {
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
        Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
        
        @Override
        public BatchMeta emitPartitionBatchNew(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta lastMeta) {
            SimpleConsumer consumer = connect(partition);

            long offset = 0;
            if(lastMeta!=null) {
                offset = lastMeta.nextOffset;
            }
            
            ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, partition % _config.partitionsPerHost, offset, _config.fetchSizeBytes));
            long endoffset = offset;
            for(MessageAndOffset msg: msgs) {
                emit(attempt, collector, msg.message());
                endoffset = msg.offset();
            }
            BatchMeta newMeta = new BatchMeta();
            newMeta.offset = offset;
            newMeta.nextOffset = endoffset;
            return newMeta;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta meta) {
            SimpleConsumer consumer = connect(partition);
                        
            ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, partition % _config.partitionsPerHost, meta.offset, _config.fetchSizeBytes));
            long offset = meta.offset;
            for(MessageAndOffset msg: msgs) {
                if(offset == meta.nextOffset) break;
                if(offset > meta.nextOffset) {
                    throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                }
                emit(attempt, collector, msg.message());
                offset = msg.offset();
                
            }            
        }
        
        private void emit(TransactionAttempt attempt, BatchOutputCollector collector, Message msg) {
                List<Object> values = _config.scheme.deserialize(Utils.toByteArray(msg.payload()));
                List<Object> toEmit = new ArrayList<Object>();
                toEmit.add(attempt);
                toEmit.addAll(values);
                collector.emit(toEmit);            
        }

        private SimpleConsumer connect(int partition) {
            if(!_kafka.containsKey(partition)) {
                int hostIndex = partition % _config.hosts.size();
                _kafka.put(partition, new SimpleConsumer(_config.hosts.get(hostIndex), _config.port, _config.socketTimeoutMs, _config.bufferSizeBytes));
            }   
            return _kafka.get(partition);
        }
        
        @Override
        public void close() {
            for(SimpleConsumer consumer: _kafka.values()) {
                consumer.close();
            }
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