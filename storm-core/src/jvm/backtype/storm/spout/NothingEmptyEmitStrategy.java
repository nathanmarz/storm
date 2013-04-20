package backtype.storm.spout;

import java.util.Map;

public class NothingEmptyEmitStrategy implements ISpoutWaitStrategy {
    @Override
    public void emptyEmit(long streak) {        
    }

    @Override
    public void prepare(Map conf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
