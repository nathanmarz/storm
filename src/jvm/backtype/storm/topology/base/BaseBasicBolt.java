package backtype.storm.topology.base;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import java.util.Map;

public abstract class BaseBasicBolt extends BaseComponent implements IBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    
}
