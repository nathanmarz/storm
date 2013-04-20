package backtype.storm.state;

import backtype.storm.tuple.Tuple;

public interface ISubscribedState {
    void set(Object id, Tuple tuple);
    void remove(Object id);
}
