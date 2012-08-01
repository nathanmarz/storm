package storm.trident.partition;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IdentityGrouping implements CustomStreamGrouping {

    List<Integer> ret = new ArrayList<Integer>();
    Map<Integer, List<Integer>> _precomputed = new HashMap();
    
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> tasks) {
        List<Integer> sourceTasks = new ArrayList<Integer>(context.getComponentTasks(stream.get_componentId()));
        Collections.sort(sourceTasks);
        if(sourceTasks.size()!=tasks.size()) {
            throw new RuntimeException("Can only do an identity grouping when source and target have same number of tasks");
        }
        tasks = new ArrayList<Integer>(tasks);
        Collections.sort(tasks);
        for(int i=0; i<sourceTasks.size(); i++) {
            int s = sourceTasks.get(i);
            int t = tasks.get(i);
            _precomputed.put(s, Arrays.asList(t));
        }
    }

    @Override
    public List<Integer> chooseTasks(int task, List<Object> values) {
        List<Integer> ret = _precomputed.get(task);
        if(ret==null) {
            throw new RuntimeException("Tuple emitted by task that's not part of this component. Should be impossible");
        }
        return ret;
    }
    
}
