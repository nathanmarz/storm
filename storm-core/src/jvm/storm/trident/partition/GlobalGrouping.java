package storm.trident.partition;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GlobalGrouping implements CustomStreamGrouping {

    List<Integer> target;
    
    
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targets) {
        List<Integer> sorted = new ArrayList<Integer>(targets);
        Collections.sort(sorted);
        target = Arrays.asList(sorted.get(0));
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        return target;
    }
    
}
