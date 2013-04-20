package storm.trident.state.map;

import java.util.List;
import storm.trident.state.ValueUpdater;

public interface MapState<T> extends ReadOnlyMapState<T> {
    List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters);
    void multiPut(List<List<Object>> keys, List<T> vals);
}
