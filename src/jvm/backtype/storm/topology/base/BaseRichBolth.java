package backtype.storm.topology.base;

import backtype.storm.topology.IRichbolth;

public abstract class BaseRichbolth extends BaseComponent implements IRichbolth {
    @Override
    public void cleanup() {
    }    
}
