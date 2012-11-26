package storm.trident.state.map;

import java.util.List;


public interface IBackingMap<T> {
    List<T> multiGet(List<? extends List<Object>> keys);
    void multiPut(List<? extends List<Object>> keys, List<T> vals);
}
