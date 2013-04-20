package storm.trident.state.map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.ReducerValueUpdater;
import storm.trident.state.StateUpdater;
import storm.trident.state.ValueUpdater;
import storm.trident.tuple.ComboList;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class MapReducerAggStateUpdater implements StateUpdater<MapState> {
    ReducerAggregator _agg;
    Fields _groupFields;
    Fields _inputFields;
    ProjectionFactory _groupFactory;
    ProjectionFactory _inputFactory;
    ComboList.Factory _factory;
    
    
    public MapReducerAggStateUpdater(ReducerAggregator agg, Fields groupFields, Fields inputFields) {
        _agg = agg;
        _groupFields = groupFields;
        _inputFields = inputFields;
        _factory = new ComboList.Factory(groupFields.size(), 1);
    }
    

    @Override
    public void updateState(MapState map, List<TridentTuple> tuples, TridentCollector collector) {
        Map<List<Object>, List<TridentTuple>> grouped = new HashMap();
        
        List<List<Object>> groups = new ArrayList<List<Object>>(tuples.size());
        List<Object> values = new ArrayList<Object>(tuples.size());
        for(TridentTuple t: tuples) {
            List<Object> group = _groupFactory.create(t);
            List<TridentTuple> groupTuples = grouped.get(group);
            if(groupTuples==null) {
                groupTuples = new ArrayList();
                grouped.put(group, groupTuples);
            }
            groupTuples.add(_inputFactory.create(t));
        }
        List<List<Object>> uniqueGroups = new ArrayList(grouped.keySet());
        List<ValueUpdater> updaters = new ArrayList(uniqueGroups.size());
        for(List<Object> group: uniqueGroups) {
            updaters.add(new ReducerValueUpdater(_agg, grouped.get(group)));
        }
        List<Object> results = map.multiUpdate(uniqueGroups, updaters);

        for(int i=0; i<uniqueGroups.size(); i++) {
            List<Object> group = uniqueGroups.get(i);
            Object result = results.get(i);
            collector.emit(_factory.create(new List[] {group, new Values(result) }));
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _groupFactory = context.makeProjectionFactory(_groupFields);
        _inputFactory = context.makeProjectionFactory(_inputFields);
    }

    @Override
    public void cleanup() {
    }
    
}
