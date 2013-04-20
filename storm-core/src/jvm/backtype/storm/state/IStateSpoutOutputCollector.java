package backtype.storm.state;

public interface IStateSpoutOutputCollector extends ISynchronizeOutputCollector {
    void remove(int streamId, Object id);
}
