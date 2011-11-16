package backtype.storm.grouping;

import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.List;

public interface CustomStreamGrouping extends Serializable {
    
   /**
     * Tells the stream grouping at runtime the number of tasks in the target bolt.
     * This information should be used in taskIndicies to determine the target tasks.
     */
   void prepare(int numTasks);
    
   /**
     * This function implements a custom stream grouping. It takes in as input
     * the number of tasks in the target bolt in prepare and returns the
     * indices of the tasks to send the tuple to. Each index must be in the range
     * [0, numTargetTasks-1]
     * 
     * @param tuple the tuple to group on
     * @param numTargetTasks the number of tasks in the target bolt
     */
   List<Integer> taskIndices(Tuple tuple); 
}
