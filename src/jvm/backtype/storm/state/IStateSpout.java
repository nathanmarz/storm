package backtype.storm.state;

import backtype.storm.task.TopologyContext;
import java.io.Serializable;
import java.util.Map;

public interface IStateSpout extends Serializable {
    void open(Map conf, TopologyContext context);
    void close();
    void nextTuple(StateSpoutOutputCollector collector);
    void synchronize(SynchronizeOutputCollector collector);
}
