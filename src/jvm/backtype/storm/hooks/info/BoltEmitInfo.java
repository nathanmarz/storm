package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;

public class BoltEmitInfo {
    public List<Object> values;
    public String stream;
    public Integer directTask;
    public Collection<Tuple> anchors;
    public List<Integer> outTasks;
    
    public BoltEmitInfo(List<Object> values, String stream, Integer directTask,
            Collection<Tuple> anchors, List<Integer> outTasks) {
        this.values = values;
        this.stream = stream;
        this.directTask = directTask;
        this.anchors = anchors;
        this.outTasks = outTasks;
    }
}
