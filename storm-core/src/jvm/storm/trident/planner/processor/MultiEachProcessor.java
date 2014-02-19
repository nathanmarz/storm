package storm.trident.planner.processor;

import java.util.Map;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.tuple.Fields;
import backtype.storm.task.TopologyContext;
import storm.trident.planner.TupleReceiver;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.operation.MultiFunction;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.MultiOutputFactory;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;

/**
   For working with multiple output streams.
 */
public class MultiEachProcessor implements TridentProcessor {
    
    public static Logger LOG = LoggerFactory.getLogger(MultiEachProcessor.class);
    
    MultiFunction _function;
    TridentContext _context;
    MultiStreamCollector _collector;
    Fields _inputFields;
    ProjectionFactory _projection;
    MultiOutputMapping _outputMap;
    MultiOutputFactory _outputFactory;

    public MultiEachProcessor(Fields inputFields, MultiFunction function, MultiOutputMapping outputMap) {
        _function = function;
        _inputFields = inputFields;
        _outputMap = outputMap;                
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("MultiEach operation can only have one parent");
        }
        _context = tridentContext;
        _collector = new MultiStreamCollector(tridentContext, _outputMap);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _function.prepare(conf, new TridentOperationContext(context, _projection));
        _outputFactory = new MultiOutputFactory(parents.get(0), _outputMap);
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
        return _outputFactory;
    }

    /**
       Not intended for use outside of the multi each context. Pulls the
       parent output factory that corresponds to its input stream
     */
    public static class MultiEachProcessorChild extends ProjectedProcessor {

        String _streamName;
        
        public MultiEachProcessorChild(Fields projectFields, String streamName) {
            super(projectFields);
            _streamName = streamName;
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
            if(tridentContext.getParentTupleFactories().size()!=1) {
                throw new RuntimeException("MultiEachProcessorChild processor can only have one parent");
            }
            _context = tridentContext;
        
            Factory parentFactory = tridentContext.getParentTupleFactories().get(0); 
            OperationOutputFactory realParent = ((MultiOutputFactory)parentFactory).getFactory(_streamName);
            
            _factory = new ProjectionFactory(realParent, _projectFields);
        }
    }
}
