package backtype.storm.topology;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.List;


public class BasicOutputCollector implements IBasicOutputCollector {
    private OutputCollector out;
    private Tuple inputTuple;

    public BasicOutputCollector(OutputCollector out) {
        this.out = out;
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, inputTuple, tuple);
    }

    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public void setContext(Tuple inputTuple) {
        this.inputTuple = inputTuple;
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        out.emitDirect(taskId, streamId, inputTuple, tuple);
    }

    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }

    protected IOutputCollector getOutputter() {
        return out;
    }

    public void reportError(Throwable t) {
        out.reportError(t);
    }
}
