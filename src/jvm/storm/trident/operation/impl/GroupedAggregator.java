package storm.trident.operation.impl;

import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.ComboList;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class GroupedAggregator implements Aggregator<Object[]> {
    ProjectionFactory _groupFactory;
    ProjectionFactory _inputFactory;
    Aggregator _agg;
    ComboList.Factory _fact;
    Fields _inFields;
    Fields _groupFields;
    
    public GroupedAggregator(Aggregator agg, Fields group, Fields input, int outSize) {
        _groupFields = group;
        _inFields = input;
        _agg = agg;
        int[] sizes = new int[2];
        sizes[0] = _groupFields.size();
        sizes[1] = outSize;
        _fact = new ComboList.Factory(sizes);
    }
    
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _inputFactory = context.makeProjectionFactory(_inFields);
        _groupFactory = context.makeProjectionFactory(_groupFields);
        _agg.prepare(conf, new TridentOperationContext(context, _inputFactory));
    }

    @Override
    public Object[] init(Object batchId, TridentCollector collector) {
        return new Object[] {new GroupCollector(collector, _fact), new HashMap(), batchId};
    }

    @Override
    public void aggregate(Object[] arr, TridentTuple tuple, TridentCollector collector) {
        GroupCollector groupColl = (GroupCollector) arr[0];
        Map<List, Object> val = (Map) arr[1];
        TridentTuple group = _groupFactory.create((TridentTupleView) tuple);
        TridentTuple input = _inputFactory.create((TridentTupleView) tuple);
        Object curr;
        if(!val.containsKey(group)) {
            curr = _agg.init(arr[2], groupColl);
            val.put((List) group, curr);
        } else {
            curr = val.get(group);
        }
        groupColl.currGroup = group;
        _agg.aggregate(curr, input, groupColl);
        
    }

    @Override
    public void complete(Object[] arr, TridentCollector collector) {
        Map<List, Object> val = (Map) arr[1];        
        GroupCollector groupColl = (GroupCollector) arr[0];
        for(Entry<List, Object> e: val.entrySet()) {
            groupColl.currGroup = e.getKey();
            _agg.complete(e.getValue(), groupColl);
        }
    }

    @Override
    public void cleanup() {
        _agg.cleanup();
    }
    
}
