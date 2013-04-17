package storm.trident.planner;

import backtype.storm.task.TopologyContext;
import java.io.Serializable;
import java.util.Map;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple.Factory;

public interface TridentProcessor extends Serializable, TupleReceiver {
    
    // imperative that don't emit any tuples from here, since output factory cannot be gotten until
    // preparation is done, therefore, receivers won't be ready to receive tuples yet
    // can't emit tuples from here anyway, since it's not within a batch context (which is only
    // startBatch, execute, and finishBatch
    void prepare(Map conf, TopologyContext context, TridentContext tridentContext);
    void cleanup();
    
    void startBatch(ProcessorContext processorContext);
    
    void finishBatch(ProcessorContext processorContext);
    
    Factory getOutputFactory();
}
