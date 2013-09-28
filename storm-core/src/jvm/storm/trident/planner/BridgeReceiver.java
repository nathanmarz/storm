package storm.trident.planner;

import backtype.storm.coordination.BatchOutputCollector;
import storm.trident.tuple.ConsList;
import storm.trident.tuple.TridentTuple;


public class BridgeReceiver implements TupleReceiver {

    BatchOutputCollector _collector;
    
    public BridgeReceiver(BatchOutputCollector collector) {
        _collector = collector;
    }
    
    @Override
    public void execute(ProcessorContext context, String streamId, TridentTuple tuple) {
        _collector.emit(streamId, new ConsList(context.batchId, tuple));
    }
    
}
