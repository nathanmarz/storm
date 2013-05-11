package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.*;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<Map<String, List>, GlobalPartitionId, Map> {
    public static final Logger LOG = LoggerFactory.getLogger(OpaqueTridentKafkaSpout.class);
    
    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    
    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        return new storm.kafka.trident.Coordinator(conf, _config);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    
    class Emitter implements IOpaquePartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;

        public Emitter(Map conf, TopologyContext context) {
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            try {
                SimpleConsumer consumer = _connections.register(partition);
                Map ret = KafkaUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
                _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long)ret.get("offset"));
                return ret;
            } catch(FailedFetchException e) {
                LOG.warn("Failed to fetch from partition " + partition);
                if(lastMeta==null) {
                    return null;
                } else {
                    Map ret = new HashMap();
                    ret.put("offset", lastMeta.get("nextOffset"));
                    ret.put("nextOffset", lastMeta.get("nextOffset"));
                    ret.put("partition", partition.partition);
                    ret.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
                    ret.put("topic", _config.topic);
                    ret.put("topology", ImmutableMap.of("name", _topologyName, "id", _topologyInstanceId));                    
                    
                    return ret;
                }
            }
        }

        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(Map<String, List> partitions) {
            return KafkaUtils.getOrderedPartitions(partitions);
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }    
}
