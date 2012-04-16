package backtype.storm.testing;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NGrouping implements CustomStreamGrouping {
    int _n;
    List<Integer> _outTasks;
    
    public NGrouping(int n) {
        _n = n;
    }
    
    @Override
    public void prepare(TopologyContext context, Fields outFields, List<Integer> targetTasks) {
        targetTasks = new ArrayList<Integer>(targetTasks);
        Collections.sort(targetTasks);
        _outTasks = new ArrayList<Integer>();
        for(int i=0; i<_n; i++) {
            _outTasks.add(targetTasks.get(i));
        }
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
        return _outTasks;
    }
    
}
