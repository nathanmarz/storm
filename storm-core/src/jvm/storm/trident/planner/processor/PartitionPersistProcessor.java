package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentOperationContext;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class PartitionPersistProcessor implements TridentProcessor {
    StateUpdater _updater;
    State _state;
    String _stateId;
    TridentContext _context;
    Fields _inputFields;
    ProjectionFactory _projection;
    FreshCollector _collector;

    public PartitionPersistProcessor(String stateId, Fields inputFields, StateUpdater updater) {
        _updater = updater;
        _stateId = stateId;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("Partition persist operation can only have one parent");
        }
        _context = tridentContext;
        _state = (State) context.getTaskData(_stateId);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _collector = new FreshCollector(tridentContext);
        _updater.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _updater.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        processorContext.state[_context.getStateIndex()] = new ArrayList<TridentTuple>();        
    }
    
    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        ((List) processorContext.state[_context.getStateIndex()]).add(_projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        Object batchId = processorContext.batchId;
        // since this processor type is a committer, this occurs in the commit phase
        List<TridentTuple> buffer = (List) processorContext.state[_context.getStateIndex()];
        
        // don't update unless there are tuples
        // this helps out with things like global partition persist, where multiple tasks may still
        // exist for this processor. Only want the global one to do anything
        // this is also a helpful optimization that state implementations don't need to manually do
        if(buffer.size() > 0) {
            Long txid = null;
            // this is to support things like persisting off of drpc stream, which is inherently unreliable
            // and won't have a tx attempt
            if(batchId instanceof TransactionAttempt) {
                txid = ((TransactionAttempt) batchId).getTransactionId();
            }
            _state.beginCommit(txid);
            _updater.updateState(_state, buffer, _collector);
            _state.commit(txid);
        }
    }    

    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    } 
}