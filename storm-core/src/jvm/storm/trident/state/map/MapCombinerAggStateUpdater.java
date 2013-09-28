package storm.trident.state.map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.CombinerValueUpdater;
import storm.trident.state.StateUpdater;
import storm.trident.state.ValueUpdater;
import storm.trident.tuple.ComboList;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class MapCombinerAggStateUpdater implements StateUpdater<MapState> {
    CombinerAggregator _agg;
    Fields _groupFields;
    Fields _inputFields;
    ProjectionFactory _groupFactory;
    ProjectionFactory _inputFactory;
    ComboList.Factory _factory;
    
    
    public MapCombinerAggStateUpdater(CombinerAggregator agg, Fields groupFields, Fields inputFields) {
        _agg = agg;
        _groupFields = groupFields;
        _inputFields = inputFields;
        if(inputFields.size()!=1) {
            throw new IllegalArgumentException("Combiner aggs only take a single field as input. Got this instead: " + inputFields.toString());
        }
        _factory = new ComboList.Factory(groupFields.size(), inputFields.size());
    }
    

    @Override
    public void updateState(MapState map, List<TridentTuple> tuples, TridentCollector collector) {
        List<List<Object>> groups = new ArrayList<List<Object>>(tuples.size());
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>(tuples.size());
                
        for(TridentTuple t: tuples) {
            groups.add(_groupFactory.create(t));
            updaters.add(new CombinerValueUpdater(_agg,_inputFactory.create(t).getValue(0)));
        }
        List<Object> newVals = map.multiUpdate(groups, updaters);
       
        for(int i=0; i<tuples.size(); i++) {
            List<Object> key = groups.get(i);
            Object result = newVals.get(i);            
            collector.emit(_factory.create(new List[] {key, new Values(result) }));
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
