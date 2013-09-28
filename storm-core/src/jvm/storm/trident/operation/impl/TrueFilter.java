package storm.trident.operation.impl;

import java.util.Map;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TrueFilter implements Filter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return true;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
    
}
