package backtype.storm.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

public class Fields implements Iterable<String>, Serializable {
    private List<String> _fields;
    private Map<String, Integer> _index = new HashMap<String, Integer>();
    
    public Fields(String... fields) {
        this(Arrays.asList(fields));
    }
    
    public Fields(List<String> fields) {
        _fields = new ArrayList<String>(fields.size());
        for (String field : fields) {
            if (_fields.contains(field))
                throw new IllegalArgumentException(
                    String.format("duplicate field '%s'", field)
                );
            _fields.add(field);
        }
        index();
    }
    
    public List<Object> select(Fields selector, List<Object> tuple) {
        List<Object> ret = new ArrayList<Object>(selector.size());
        for(String s: selector) {
            ret.add(tuple.get(_index.get(s)));
        }
        return ret;
    }

    public List<String> toList() {
        return new ArrayList<String>(_fields);
    }
    
    public int size() {
        return _fields.size();
    }

    public String get(int index) {
        return _fields.get(index);
    }

    public Iterator<String> iterator() {
        return _fields.iterator();
    }
    
    /**
     * Returns the position of the specified field.
     */
    public int fieldIndex(String field) {
        Integer ret = _index.get(field);
        if(ret==null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }
    
    /**
     * Returns true if this contains the specified name of the field.
     */
    public boolean contains(String field) {
        return _index.containsKey(field);
    }
    
    private void index() {
        for(int i=0; i<_fields.size(); i++) {
            _index.put(_fields.get(i), i);
        }
    }

    @Override
    public String toString() {
        return _fields.toString();
    }    
}
