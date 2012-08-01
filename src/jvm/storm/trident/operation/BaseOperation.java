package storm.trident.operation;

import java.util.Map;

public class BaseOperation implements Operation {

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
    
}
