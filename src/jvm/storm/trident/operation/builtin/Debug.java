package storm.trident.operation.builtin;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class Debug extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println("DEBUG: " + tuple.toString());
        return true;
    }
    
}
