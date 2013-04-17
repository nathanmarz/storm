package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import storm.trident.operation.Function;
import storm.trident.operation.TridentOperationContext;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class EachProcessor implements TridentProcessor {
    Function _function;
    TridentContext _context;
    AppendCollector _collector;
    Fields _inputFields;
    ProjectionFactory _projection;
    
    public EachProcessor(Fields inputFields, Function function) {
        _function = function;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("Each operation can only have one parent");
        }
        _context = tridentContext;
        _collector = new AppendCollector(tridentContext);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _function.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _function.cleanup();
    }    

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        _collector.setContext(processorContext, tuple);
        _function.execute(_projection.create(tuple), _collector);
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
    }

    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    }    
}
