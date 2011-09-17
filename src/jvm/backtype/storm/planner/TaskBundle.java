package backtype.storm.planner;

import backtype.storm.task.IBolt;
import java.io.Serializable;


public class TaskBundle implements Serializable {
    public IBolt task;
    public int componentId;
    
    public TaskBundle(IBolt task, int componentId) {
        this.task = task;
        this.componentId = componentId;
    }
    
}