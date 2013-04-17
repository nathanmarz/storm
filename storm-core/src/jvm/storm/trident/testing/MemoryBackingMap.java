package storm.trident.testing;

import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryBackingMap implements IBackingMap<Object> {
    Map _vals = new HashMap();

    @Override
    public List<Object> multiGet(List<List<Object>> keys) {
        List ret = new ArrayList();
        for(List key: keys) {
            ret.add(_vals.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Object> vals) {
        for(int i=0; i<keys.size(); i++) {
            List key = keys.get(i);
            Object val = vals.get(i);
            _vals.put(key, val);
        }
    }
}
