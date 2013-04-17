package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.TupleReceiver;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class ProjectedProcessor implements TridentProcessor {
    Fields _projectFields;
    ProjectionFactory _factory;
    TridentContext _context;
    
    public ProjectedProcessor(Fields projectFields) {
        _projectFields = projectFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        if(tridentContext.getParentTupleFactories().size()!=1) {
            throw new RuntimeException("Projection processor can only have one parent");
        }
        _context = tridentContext;
        _factory = new ProjectionFactory(tridentContext.getParentTupleFactories().get(0), _projectFields);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        TridentTuple toEmit = _factory.create(tuple);
        for(TupleReceiver r: _context.getReceivers()) {
            r.execute(processorContext, _context.getOutStreamId(), toEmit);
        }
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
    }

    @Override
    public Factory getOutputFactory() {
        return _factory;
    }
}
