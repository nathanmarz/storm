package storm.kafka.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.StaticPartitionConnections;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<Map> {
    public static final Logger LOG = Logger.getLogger(OpaqueTridentKafkaSpout.class);
    
    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    
    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map map, TopologyContext tc) {
        return new Coordinator();
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator {
        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }
    }
    
    class Emitter implements IOpaquePartitionedTridentSpout.Emitter<Map> {
        StaticPartitionConnections _connections;
        
        public Emitter() {
            _connections = new StaticPartitionConnections(_config);
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, int partition, Map lastMeta) {
            try {
                SimpleConsumer consumer = _connections.getConsumer(partition);
                return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta, _topologyInstanceId);
            } catch(FailedFetchException e) {
                LOG.warn("Failed to fetch from partition " + partition);
                if(lastMeta==null) {
                    return null;
                } else {
                    Map ret = new HashMap();
                    ret.put("offset", lastMeta.get("nextOffset"));
                    ret.put("nextOffset", lastMeta.get("nextOffset"));
                    return ret;
                }
            }
        }

        @Override
        public long numPartitions() {
            StaticHosts hosts = (StaticHosts) _config.hosts;
            return hosts.hosts.size() * hosts.partitionsPerHost;
        }

        @Override
        public void close() {
            _connections.close();
        }        
    }    
}
