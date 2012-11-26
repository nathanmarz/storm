package storm.trident.operation.builtin;

import backtype.storm.tuple.Values;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;


public class MapGet<T> extends BaseQueryFunction<ReadOnlyMapState<T>, T> {
    @Override
    public List<T> batchRetrieve(ReadOnlyMapState<T> map, List<TridentTuple> keys) {
        return map.multiGet(keys);
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
