package storm.trident.state.map;

import java.util.List;
import storm.trident.state.State;

public interface RemovableMapState<T> extends State {
    void multiRemove(List<List<Object>> keys);
}
