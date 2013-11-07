package storm.trident.testing;

import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.MultiFunction;
import storm.trident.planner.processor.MultiStreamCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class DebugMultiStream implements MultiFunction {

    protected String _streamA;
    protected String _streamB;
    
    public DebugMultiStream(String streamA, String streamB) {
        _streamA = streamA;
        _streamB = streamB;
    }
    
    @Override
    public void execute(TridentTuple tuple, MultiStreamCollector collector) {
        if (tuple.getString(0).startsWith("A")) {
            collector.emitTo(_streamA, new Values("A"));
        } else {
            collector.emitTo(_streamB, new Values("B"));
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }
    
    @Override
    public void cleanup() {
    }
}
