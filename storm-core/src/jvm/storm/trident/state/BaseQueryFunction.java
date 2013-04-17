package storm.trident.state;

import storm.trident.operation.BaseOperation;


public abstract class BaseQueryFunction<S extends State, T> extends BaseOperation implements QueryFunction<S, T> {
    
}
