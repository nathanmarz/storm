package storm.trident.tuple;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;
import storm.trident.planner.processor.MultiOutputMapping;

/**
   Maintains several output factories, one per output stream,
   for use with MultiEachProcessor
 */
public class MultiOutputFactory implements TridentTuple.Factory {

    Map<String,OperationOutputFactory> _factories;
    
    public MultiOutputFactory(TridentTuple.Factory parent, MultiOutputMapping map) {
        
        _factories = new HashMap<String,OperationOutputFactory>();
        for (String streamName : map.getStreamNames()) {
            _factories.put(streamName, new OperationOutputFactory(parent, map.getFields(streamName)));
        }        
    }

    /**
       Main way of interacting with MultiOutputFactory; returns
       the factory that corresponds with the input stream name
     */
    public OperationOutputFactory getFactory(String name) {
        return _factories.get(name);
    }

    
    /** TO SATISFY FACTORY INTERFACE ONLY; UNSAFE SINCE STATE MUST BE MANAGED EXTERNALLY **/

    String _currentStream;
    
    public void setStream(String streamName) {
        _currentStream = streamName;
    }

    public TridentTuple create(TridentTupleView parent, List<Object> selfVals) {
        return _factories.get(_currentStream).create(parent, selfVals);
    }

    @Override
    public Map<String, ValuePointer> getFieldIndex() {
        return _factories.get(_currentStream).getFieldIndex();
    }

    @Override
    public int numDelegates() {
        return _factories.get(_currentStream).numDelegates();
    }

    @Override
    public List<String> getOutputFields() {
        return _factories.get(_currentStream).getOutputFields();
    }
}
