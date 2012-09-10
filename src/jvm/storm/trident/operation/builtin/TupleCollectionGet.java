package storm.trident.operation.builtin;

import backtype.storm.state.ITupleCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class TupleCollectionGet extends BaseQueryFunction<State, Object> {

    @Override
    public List<Object> batchRetrieve(State state, List<TridentTuple> args) {
        List<Object> ret = new ArrayList<Object>(args.size());
        Iterator<List<Object>> tuplesIterator = ((ITupleCollection)state).getTuples();
        for(int i=0; i<args.size(); i++) {
            ret.add(tuplesIterator);
        }
        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        Iterator<List<Object>> tuplesIterator = (Iterator<List<Object>>) result;
        while(tuplesIterator.hasNext()) {
            collector.emit(tuplesIterator.next());
        }
    }
}
