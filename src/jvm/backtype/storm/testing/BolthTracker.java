package backtype.storm.testing;

import backtype.storm.topology.IRichbolth;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.HashMap;
import java.util.Map;


public class bolthTracker extends NonRichbolthTracker implements IRichbolth {
    IRichbolth _richDelegate;

    public bolthTracker(IRichbolth delegate, String id) {
        super(delegate, id);
        _richDelegate = delegate;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _richDelegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
