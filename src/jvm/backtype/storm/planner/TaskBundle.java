package backtype.storm.planner;

import backtype.storm.task.Ibolth;
import java.io.Serializable;


public class TaskBundle implements Serializable {
    public Ibolth task;
    public int componentId;
    
    public TaskBundle(Ibolth task, int componentId) {
        this.task = task;
        this.componentId = componentId;
    }
    
}