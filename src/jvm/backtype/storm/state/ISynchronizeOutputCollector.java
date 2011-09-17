package backtype.storm.state;

import java.util.List;

public interface ISynchronizeOutputCollector {
    void add(int streamId, List<Object> tuple);
    //this will return immediately and do synchronization once the spout function exits
    //(to avoid blowing up the stack) -- no reason for user to do anything more after calling this anyway
    void resynchronize();
}
