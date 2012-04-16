package backtype.storm.hooks.info;

import java.util.Collection;
import java.util.List;

public class EmitInfo {
    public List<Object> values;
    public String stream;
    public Collection<Integer> outTasks;
    
    public EmitInfo(List<Object> values, String stream, Collection<Integer> outTasks) {
        this.values = values;
        this.stream = stream;
        this.outTasks = outTasks;
    }
}
