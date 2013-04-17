package storm.trident.tuple;

import java.util.AbstractList;
import java.util.List;

public class ConsList extends AbstractList<Object> {
    List<Object> _elems;
    Object _first;
    
    public ConsList(Object o, List<Object> elems) {
        _elems = elems;
        _first = o;
    }

    @Override
    public Object get(int i) {
        if(i==0) return _first;
        else {
            return _elems.get(i - 1);
        }
    }

    @Override
    public int size() {
        return _elems.size() + 1;
    }
}
