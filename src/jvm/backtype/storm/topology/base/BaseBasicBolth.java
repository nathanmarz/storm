package backtype.storm.topology.base;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicbolth;
import java.util.Map;

public abstract class BaseBasicbolth extends BaseComponent implements IBasicbolth {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    
}
