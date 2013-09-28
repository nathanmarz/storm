package storm.trident.operation.builtin;

import backtype.storm.tuple.Values;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;


public class MapGet extends BaseQueryFunction<ReadOnlyMapState, Object> {
    @Override
    public List<Object> batchRetrieve(ReadOnlyMapState map, List<TridentTuple> keys) {
        return map.multiGet((List) keys);
    }    
    
    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        collector.emit(new Values(result));
    }    
}
