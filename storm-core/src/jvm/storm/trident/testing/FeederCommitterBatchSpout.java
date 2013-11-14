package storm.trident.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ICommitterTridentSpout;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;


public class FeederCommitterBatchSpout implements ICommitterTridentSpout, IFeeder {

    FeederBatchSpout _spout;
    
    public FeederCommitterBatchSpout(List<String> fields) {
        _spout = new FeederBatchSpout(fields);
    }
    
    public void setWaitToEmit(boolean trueIfWait) {
        _spout.setWaitToEmit(trueIfWait);
    }
    
    static class CommitterEmitter implements ICommitterTridentSpout.Emitter {
        ITridentSpout.Emitter _emitter;
        
        
        public CommitterEmitter(ITridentSpout.Emitter e) {
            _emitter = e;
        }
        
        @Override
        public void commit(TransactionAttempt attempt) {
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            _emitter.emitBatch(tx, coordinatorMeta, collector);
        }

        @Override
        public void success(TransactionAttempt tx) {
            _emitter.success(tx);
        }

        @Override
        public void close() {
            _emitter.close();
        }
        
    }
    
    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new CommitterEmitter(_spout.getEmitter(txStateId, conf, context));
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return _spout.getCoordinator(txStateId, conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public void feed(Object tuples) {
        _spout.feed(tuples);
    }
    
}
