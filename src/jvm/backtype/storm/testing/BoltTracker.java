package backtype.storm.testing;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;


public class BoltTracker extends NonRichBoltTracker implements IRichBolt {
    IRichBolt _richDelegate;

    public BoltTracker(IRichBolt delegate, String id) {
        super(delegate, id);
        _richDelegate = delegate;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _richDelegate.declareOutputFields(declarer);
    }
}
