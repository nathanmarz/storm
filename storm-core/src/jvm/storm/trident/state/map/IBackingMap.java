package storm.trident.state.map;

import java.util.List;


public interface IBackingMap<T> {
    List<T> multiGet(List<List<Object>> keys); 
    void multiPut(List<List<Object>> keys, List<T> vals); 
}
