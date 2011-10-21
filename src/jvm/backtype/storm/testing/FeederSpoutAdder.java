package backtype.storm.testing;

import backtype.storm.drpc.SpoutAdder;
import backtype.storm.tuple.Values;

public class FeederSpoutAdder implements SpoutAdder {
    private FeederSpout _spout;
    
    public FeederSpoutAdder(FeederSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void add(String function, String args, String returnInfo) {
        _spout.feed(new Values(args, returnInfo));
    }    
}
