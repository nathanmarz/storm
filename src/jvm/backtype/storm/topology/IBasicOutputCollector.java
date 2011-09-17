package backtype.storm.topology;

import java.util.List;

public interface IBasicOutputCollector {
    List<Integer> emit(int streamId, List<Object> tuple);
    void emitDirect(int taskId, int streamId, List<Object> tuple);
}
