package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import java.util.List;

public interface IInternalOutputCollector {
    public List<Integer> emit(Tuple output);
    public void emitDirect(int taskId, Tuple output);
    public void ack(Tuple inputTuple, List<Long> generatedTupleIds);
    public void fail(Tuple inputTuple);
    public void reportError(Throwable error);
}