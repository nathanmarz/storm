package backtype.storm.hooks.info;

import java.util.List;

public class SpoutEmitInfo {
    public List<Object> values;
    public String stream;
    public Integer directTask;
    public List<Integer> outTasks;
    public Object messageId;
    
    public SpoutEmitInfo(List<Object> values, String stream, Integer directTask,
            List<Integer> outTasks, Object messageId) {
        this.values = values;
        this.stream = stream;
        this.directTask = directTask;
        this.messageId = messageId;
        this.outTasks = outTasks;
    }
}
