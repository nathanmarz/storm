package storm.trident.testing;

import java.util.List;
import org.json.simple.JSONValue;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TuplifyArgs extends BaseFunction {

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        String args = input.getString(0);
        List<List<Object>> tuples = (List) JSONValue.parse(args);
        for(List<Object> tuple: tuples) {
            collector.emit(tuple);
        }
    }
    
}
