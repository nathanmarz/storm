package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentOperationContext;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class AggregateProcessor implements TridentProcessor {
    Aggregator _agg;
    TridentContext _context;
    FreshCollector _collector;
    Fields _inputFields;
    ProjectionFactory _projection;

    public AggregateProcessor(Fields inputFields, Aggregator agg) {
        _agg = agg;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("Aggregate operation can only have one parent");
        }
        _context = tridentContext;
        _collector = new FreshCollector(tridentContext);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _agg.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _agg.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        processorContext.state[_context.getStateIndex()] = _agg.init(processorContext.batchId, _collector);
    }    

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        _collector.setContext(processorContext);
        _agg.aggregate(processorContext.state[_context.getStateIndex()], _projection.create(tuple), _collector);
    }
    
    @Override
    public void finishBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        _agg.complete(processorContext.state[_context.getStateIndex()], _collector);
    }
 
    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    }
}
