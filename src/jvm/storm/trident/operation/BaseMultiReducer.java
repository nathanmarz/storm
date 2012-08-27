package storm.trident.operation;

import java.util.Map;

public abstract class BaseMultiReducer<T> implements MultiReducer<T> {

    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
    }


    @Override
    public void cleanup() {
    }
    
}
