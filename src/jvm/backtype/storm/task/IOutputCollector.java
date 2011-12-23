package backtype.storm.task;

import backtype.storm.tuple.Tuple;

public interface IOutputCollector extends IBoltEmitter {
    void ack(Tuple input);
    void fail(Tuple input);
    void reportError(Throwable error);
}
