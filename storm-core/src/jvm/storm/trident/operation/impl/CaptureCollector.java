package storm.trident.operation.impl;

import java.util.ArrayList;
import java.util.List;
import storm.trident.operation.TridentCollector;

public class CaptureCollector implements TridentCollector {
    public List<List<Object>> captured = new ArrayList();
    
    TridentCollector _coll;
    
    public void setCollector(TridentCollector coll) {
        _coll = coll;
    }
    
    @Override
    public void emit(List<Object> values) {
        this.captured.add(values);
    }

    @Override
    public void reportError(Throwable t) {
        _coll.reportError(t);
    }
}
