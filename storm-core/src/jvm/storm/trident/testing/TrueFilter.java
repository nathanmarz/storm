package storm.trident.testing;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class TrueFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return true;
    }
    
}
