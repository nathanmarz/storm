package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentOperationContext;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.state.QueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class StateQueryProcessor implements TridentProcessor {
    QueryFunction _function;
    State _state;
    String _stateId;
    TridentContext _context;
    Fields _inputFields;
    ProjectionFactory _projection;
    AppendCollector _collector;
    
    public StateQueryProcessor(String stateId, Fields inputFields, QueryFunction function) {
        _stateId = stateId;
        _function = function;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("State query operation can only have one parent");
        }
        _context = tridentContext;
        _state = (State) context.getTaskData(_stateId);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _collector = new AppendCollector(tridentContext);
        _function.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _function.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        processorContext.state[_context.getStateIndex()] =  new BatchState();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        BatchState state = (BatchState) processorContext.state[_context.getStateIndex()];
        state.tuples.add(tuple);
        state.args.add(_projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        BatchState state = (BatchState) processorContext.state[_context.getStateIndex()];
        if(!state.tuples.isEmpty()) {
            List<Object> results = _function.batchRetrieve(_state, state.args);
            if(results.size()!=state.tuples.size()) {
                throw new RuntimeException("Results size is different than argument size: " + results.size() + " vs " + state.tuples.size());
            }
            for(int i=0; i<state.tuples.size(); i++) {
                TridentTuple tuple = state.tuples.get(i);
                Object result = results.get(i);
                _collector.setContext(processorContext, tuple);
                _function.execute(_projection.create(tuple), result, _collector);            
            }
        }
    }
    
    private static class BatchState {
        public List<TridentTuple> tuples = new ArrayList<TridentTuple>();
        public List<TridentTuple> args = new ArrayList<TridentTuple>();
    }

    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    } 
}
