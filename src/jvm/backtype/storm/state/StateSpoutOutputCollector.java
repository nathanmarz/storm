package backtype.storm.state;


public class StateSpoutOutputCollector extends SynchronizeOutputCollector implements IStateSpoutOutputCollector {

    @Override
    public void remove(int streamId, Object id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
