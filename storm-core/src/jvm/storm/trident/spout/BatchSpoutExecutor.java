package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.trident.operation.TridentCollector;

public class BatchSpoutExecutor implements ITridentSpout {
    public static class EmptyCoordinator implements BatchCoordinator {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }
    }
    
    public class BatchSpoutEmitter implements Emitter {

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            _spout.emitBatch(tx.getTransactionId(), collector);
        }        
        
        @Override
        public void success(TransactionAttempt tx) {
            _spout.ack(tx.getTransactionId());
        }

        @Override
        public void close() {
            _spout.close();
        }        
    }
    
    IBatchSpout _spout;
    
    public BatchSpoutExecutor(IBatchSpout spout) {
        _spout = spout;
    }
    
    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new EmptyCoordinator();
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        _spout.open(conf, context);
        return new BatchSpoutEmitter();
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }
    
}
