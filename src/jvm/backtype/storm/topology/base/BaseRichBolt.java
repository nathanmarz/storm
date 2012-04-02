package backtype.storm.topology.base;

import backtype.storm.topology.IRichBolt;

public abstract class BaseRichBolt extends BaseComponent implements IRichBolt {
    @Override
    public void cleanup() {
    }    
}
