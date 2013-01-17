package backtype.storm.metric.api;

import backtype.storm.metric.api.IMetric;
import java.util.HashMap;
import java.util.Map;

public class MultiCountMetric implements IMetric, java.io.Serializable {
    Map<String, CountMetric> _value = new HashMap();

    public MultiCountMetric() {
    }
    
    public CountMetric scope(String key) {
        CountMetric val = _value.get(key);
        if(val == null) {
            _value.put(key, val = new CountMetric());
        }
        return val;
    }

    public Object getValueAndReset() {
        Map ret = new HashMap();
        for(Map.Entry<String, CountMetric> e : _value.entrySet()) {
            ret.put(e.getKey(), e.getValue().getValueAndReset());
        }
        return ret;
    }
}
