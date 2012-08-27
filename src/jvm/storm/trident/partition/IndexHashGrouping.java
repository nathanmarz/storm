package storm.trident.partition;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.Arrays;
import java.util.List;

public class IndexHashGrouping implements CustomStreamGrouping {
    public static int objectToIndex(Object val, int numPartitions) {
        if(val==null) return 0;
        else {
            return Math.abs(val.hashCode()) % numPartitions;
        }
    }
    
    int _index;
    List<Integer> _targets;
    
    public IndexHashGrouping(int index) {
        _index = index;
    }
    
    
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targets = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int fromTask, List<Object> values) {
        int i = objectToIndex(values.get(_index), _targets.size());
        return Arrays.asList(_targets.get(i));
    }
    
}
