package backtype.storm.hooks.info;

import java.util.Collection;
import java.util.List;

public class EmitInfo {
    public List<Object> values;
    public String stream;
    public int taskId;
    public Collection<Integer> outTasks;
    
    public EmitInfo(List<Object> values, String stream, int taskId, Collection<Integer> outTasks) {
        this.values = values;
        this.stream = stream;
        this.taskId = taskId;
        this.outTasks = outTasks;
    }
}
