package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<GlobalPartitionInformation, GlobalPartitionId, Map> {
    
    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }
    

    
    class Emitter implements IPartitionedTridentSpout.Emitter<GlobalPartitionInformation, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        TopologyContext _context;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;


        public Emitter(Map conf, TopologyContext context) {
			IBrokerReader brokerReader = KafkaUtils.makeBrokerReader(conf, _config);
            _connections = new DynamicPartitionConnections(_config, brokerReader);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _context = context;
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);

        }
        
        @Override
        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            SimpleConsumer consumer = _connections.register(partition);
            Map ret = KafkaUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
            _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long)ret.get("offset"));
            return ret;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map meta) {
            String instanceId = (String) meta.get("instanceId");
            if(!_config.forceFromStart || instanceId.equals(_topologyInstanceId)) {
                SimpleConsumer consumer = _connections.register(partition);
                long offset = (Long) meta.get("offset");
                long nextOffset = (Long) meta.get("nextOffset");
                long start = System.nanoTime();
                ByteBufferMessageSet msgs = consumer.fetch(new FetchRequestBuilder().addFetch(_config.topic, partition.partition, offset, _config.fetchSizeBytes).build()).messageSet(_config.topic, partition.partition);
                long end = System.nanoTime();
                long millis = (end - start) / 1000000;
                _kafkaMeanFetchLatencyMetric.update(millis);
                _kafkaMaxFetchLatencyMetric.update(millis);

                for(MessageAndOffset msg: msgs) {
                    if(offset == nextOffset) break;
                    if(offset > nextOffset) {
                        throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                    }
                    KafkaUtils.emit(_config, collector, msg.message());
                    offset = msg.nextOffset();
                }        
            }
        }
        
        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(GlobalPartitionInformation partitions) {
			return partitions.getOrderedPartitions();
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }
    

    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new storm.kafka.trident.Coordinator(conf, _config);
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }
        
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}