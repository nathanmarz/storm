package backtype.storm.state;

import java.util.List;


public class SynchronizeOutputCollector implements ISynchronizeOutputCollector {

    @Override
    public void add(int streamId, Object id, List<Object> tuple) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
