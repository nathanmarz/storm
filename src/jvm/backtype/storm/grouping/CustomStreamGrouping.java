package backtype.storm.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import java.io.Serializable;
import java.util.List;

public interface CustomStreamGrouping extends Serializable {
    
   /**
     * Tells the stream grouping at runtime the tasks in the target bolt.
     * This information should be used in chooseTasks to determine the target tasks.
     * 
     * It also tells the grouping the metadata on the stream this grouping will be used on.
     */
   void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks);
    
   /**
     * This function implements a custom stream grouping. It takes in as input
     * the number of tasks in the target bolt in prepare and returns the
     * tasks to send the tuples to.
     * 
     * @param values the values to group on
     */
   List<Integer> chooseTasks(int taskId, List<Object> values); 
}
