package backtype.storm.grouping;

import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.List;

public interface CustomStreamGrouping extends Serializable {
    
   /**
     * Tells the stream grouping at runtime the number of tasks in the target bolt.
     * This information should be used in taskIndicies to determine the target tasks.
     * 
     * It also tells the grouping the metadata on the stream this grouping will be used on.
     */
   void prepare(Fields outFields, int numTasks);
    
   /**
     * This function implements a custom stream grouping. It takes in as input
     * the number of tasks in the target bolt in prepare and returns the
     * indices of the tasks to send the tuple to. Each index must be in the range
     * [0, numTargetTasks-1]
     * 
     * @param tuple the values to group on
     * @param numTargetTasks the number of tasks in the target bolt
     */
   List<Integer> taskIndices(List<Object> values); 
}
