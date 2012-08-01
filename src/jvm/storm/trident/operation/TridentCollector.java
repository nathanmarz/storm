package storm.trident.operation;

import java.util.List;


public interface TridentCollector {
    void emit(List<Object> values);
    void reportError(Throwable t);
}
