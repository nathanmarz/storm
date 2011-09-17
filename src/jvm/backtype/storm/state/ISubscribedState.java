package backtype.storm.state;

import backtype.storm.tuple.Tuple;

public interface ISubscribedState {
    void add(Tuple tuple);
    void remove(Tuple tuple);
}
