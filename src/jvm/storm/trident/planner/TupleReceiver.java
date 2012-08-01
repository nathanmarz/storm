package storm.trident.planner;

import storm.trident.tuple.TridentTuple;


public interface TupleReceiver {
    //streaId indicates where tuple came from
    void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple);
    
}
