package storm.trident.state;

import storm.trident.operation.BaseOperation;


public abstract class BaseStateUpdater<S extends State> extends BaseOperation implements StateUpdater<S> {
    
}
