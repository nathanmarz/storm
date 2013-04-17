package backtype.storm.state;

import java.util.List;

public interface ISynchronizeOutputCollector {
    void add(int streamId, Object id, List<Object> tuple);    
}
