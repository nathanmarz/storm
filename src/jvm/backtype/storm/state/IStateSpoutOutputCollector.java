package backtype.storm.state;

import java.util.List;

public interface IStateSpoutOutputCollector extends ISynchronizeOutputCollector {
    void remove(int streamId, List<Object> tuple);
}
