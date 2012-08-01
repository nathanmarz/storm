package storm.trident.operation.impl;

import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.ComboList;

public class GroupCollector implements TridentCollector {
    public List<Object> currGroup;
    
    ComboList.Factory _factory;
    TridentCollector _collector;
    
    public GroupCollector(TridentCollector collector, ComboList.Factory factory) {
        _factory = factory;
        _collector = collector;
    }
    
    @Override
    public void emit(List<Object> values) {
        List[] delegates = new List[2];
        delegates[0] = currGroup;
        delegates[1] = values;
        _collector.emit(_factory.create(delegates));
    }

    @Override
    public void reportError(Throwable t) {
        _collector.reportError(t);
    }
    
}
