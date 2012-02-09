package backtype.storm.testing;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;

public class NGrouping implements CustomStreamGrouping {
    int _n;
    
    public NGrouping(int n) {
        _n = n;
    }
    
    @Override
    public void prepare(Fields outFields, int numTasks) {
    }

    @Override
    public List<Integer> taskIndices(List<Object> values) {
        List<Integer> ret = new ArrayList<Integer>();
        for(int i=0; i<_n; i++) {
            ret.add(i);
        }
        return ret;
    }
    
}
