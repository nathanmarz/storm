package storm.kafka;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;


public class OpaqueTransactionalKafkaSpout implements IOpaquePartitionedTransactionalSpout<BatchMeta> {
    public static final Logger LOG = Logger.getLogger(OpaqueTransactionalKafkaSpout.class);
    
    public static final String ATTEMPT_FIELD = OpaqueTransactionalKafkaSpout.class.getCanonicalName() + "/attempt";

    KafkaConfig _config;
    
    public OpaqueTransactionalKafkaSpout(KafkaConfig config) {
        _config = config;
    }
    
    @Override
    public IOpaquePartitionedTransactionalSpout.Emitter<BatchMeta> getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }
    
    @Override
    public IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(Map map, TopologyContext tc) {
        return new Coordinator();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<String>(_config.scheme.getOutputFields().toList());
        fields.add(0, ATTEMPT_FIELD);
        declarer.declare(new Fields(fields));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
    
    class Coordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
        @Override
        public boolean isReady() {
            //TODO: can do a more sophisticated strategy by looking at the high water marks for each partition
            return true;
        }        
    }
    
    class Emitter implements IOpaquePartitionedTransactionalSpout.Emitter<BatchMeta> {
        KafkaPartitionConnections _connections;
        
        public Emitter() {
            _connections = new KafkaPartitionConnections(_config);
        }

        @Override
        public BatchMeta emitPartitionBatch(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta lastMeta) {
            try {
                SimpleConsumer consumer = _connections.getConsumer(partition);
                return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta);
            } catch(FailedFetchException e) {
                LOG.warn("Failed to fetch from partition " + partition);
                if(lastMeta==null) {
                    return null;
                } else {
                    BatchMeta ret = new BatchMeta();
                    ret.offset = lastMeta.nextOffset;
                    ret.nextOffset = lastMeta.nextOffset;
                    return ret;
                }
            }
        }

        @Override
        public int numPartitions() {
            return _config.hosts.size() * _config.partitionsPerHost;
        }

        @Override
        public void close() {
            _connections.close();
        }        
    }    
}
