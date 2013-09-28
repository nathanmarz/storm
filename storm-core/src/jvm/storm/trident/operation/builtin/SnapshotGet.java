package storm.trident.operation.builtin;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;

public class SnapshotGet extends BaseQueryFunction<ReadOnlySnapshottable, Object> {

    @Override
    public List<Object> batchRetrieve(ReadOnlySnapshottable state, List<TridentTuple> args) {
        List<Object> ret = new ArrayList<Object>(args.size());
        Object snapshot = state.get();
        for(int i=0; i<args.size(); i++) {
            ret.add(snapshot);
        }
        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
