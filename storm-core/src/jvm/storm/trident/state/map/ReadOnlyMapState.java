package storm.trident.state.map;

import java.util.List;
import storm.trident.state.State;

public interface ReadOnlyMapState<T> extends State {
    // certain states might only accept one-tuple keys - those should just throw an error 
    List<T> multiGet(List<List<Object>> keys);
}
