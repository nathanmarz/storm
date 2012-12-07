package backtype.storm.metric.api;

import backtype.storm.metric.api.IMetric;
import java.util.HashMap;
import java.util.Map;

public class MultiReducedMetric implements IMetric {
    Map<String, ReducedMetric> _value = new HashMap();
    IReducer _reducer;

    public MultiReducedMetric(IReducer reducer) {
        _reducer = reducer;
    }
    
    public ReducedMetric scope(String key) {
        ReducedMetric val = _value.get(key);
        if(val == null) {
            _value.put(key, val = new ReducedMetric(_reducer));
        }
        return val;
    }

    public Object getValueAndReset() {
        Map ret = new HashMap();
        for(Map.Entry<String, ReducedMetric> e : _value.entrySet()) {
            Object val = e.getValue().getValueAndReset();
            if(val != null) {
                ret.put(e.getKey(), val);
            }
        }
        return ret;
    }
}
