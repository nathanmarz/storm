package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.List;

/**
 * This output collector exposes the API for emitting tuples from an IRichBolt.
 * This is the core API for emitting tuples. For a simpler API, and a more restricted
 * form of stream processing, see IBasicBolt and BasicOutputCollector.
 */
public abstract class OutputCollector implements IOutputCollector {

    /**
     * Emits a new tuple to a specific stream with a single anchor.
     *
     * @param streamId the stream to emit to
     * @param anchor the tuple to anchor to
     * @param tuple the new output tuple from this bolt
     * @return the list of task ids that this new tuple was sent to
     */
    public List<Integer> emit(int streamId, Tuple anchor, List<Object> tuple) {
        return emit(streamId, Arrays.asList(anchor), tuple);
    }

    /**
     * Emits a new unanchored tuple to the specified stream. Beacuse it's unanchored,
     * if a failure happens downstream, this new tuple won't affect whether any
     * spout tuples are considered failed or not.
     * 
     * @param streamId the stream to emit to
     * @param tuple the new output tuple from this bolt
     * @return the list of task ids that this new tuple was sent to
     */
    public List<Integer> emit(int streamId, List<Object> tuple) {
        return emit(streamId, (List) null, tuple);
    }

    /**
     * Emits a new tuple to the default stream anchored on a group of input tuples.
     * 
     * @param anchors the tuples to anchor to
     * @param tuple the new output tuple from this bolt
     * @return the list of task ids that this new tuple was sent to
     */
    public List<Integer> emit(List<Tuple> anchors, List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }


    /**
     * Emits a new tuple to the default stream anchored on a single tuple.
     * 
     * @param anchor the tuple to anchor to
     * @param tuple the new output tuple from this bolt
     * @return the list of task ids that this new tuple was sent to
     */
    public List<Integer> emit(Tuple anchor, List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, anchor, tuple);
    }

    /**
     * Emits a new unanchored tuple to the default stream. Beacuse it's unanchored,
     * if a failure happens downstream, this new tuple won't affect whether any
     * spout tuples are considered failed or not.
     *
     * @param tuple the new output tuple from this bolt
     * @return the list of task ids that this new tuple was sent to
     */
    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    /**
     * Emits a tuple directly to the specified task id on the specified stream.
     * If the target bolt does not subscribe to this bolt using a direct grouping,
     * the tuple will not be sent. If the specified output stream is not declared
     * as direct, or the target bolt subscribes with a non-direct grouping,
     * an error will occur at runtime.
     *
     * @param taskId the taskId to send the new tuple to
     * @param streamId the stream to send the tuple on. It must be declared as a direct stream in the topology definition.
     * @param anchor the tuple to anchor to
     * @param tuple the new output tuple from this bolt
     */
    public void emitDirect(int taskId, int streamId, Tuple anchor, List<Object> tuple) {
        emitDirect(taskId, streamId, Arrays.asList(anchor), tuple);
    }

    /**
     * Emits a tuple directly to the specified task id on the specified stream.
     * If the target bolt does not subscribe to this bolt using a direct grouping,
     * the tuple will not be sent. If the specified output stream is not declared
     * as direct, or the target bolt subscribes with a non-direct grouping,
     * an error will occur at runtime. Note that this method does not use anchors,
     * so downstream failures won't affect the failure status of any spout tuples.
     *
     * @param taskId the taskId to send the new tuple to
     * @param streamId the stream to send the tuple on. It must be declared as a direct stream in the topology definition.
     * @param tuple the new output tuple from this bolt
     */
    public void emitDirect(int taskId, int streamId, List<Object> tuple) {
        emitDirect(taskId, streamId, (List) null, tuple);
    }

    /**
     * Emits a tuple directly to the specified task id on the default stream.
     * If the target bolt does not subscribe to this bolt using a direct grouping,
     * the tuple will not be sent. If the specified output stream is not declared
     * as direct, or the target bolt subscribes with a non-direct grouping,
     * an error will occur at runtime.
     *
     * <p>The default stream must be declared as direct in the topology definition.
     * See OutputDeclarer#declare for how this is done when defining topologies
     * in Java.</p>
     *
     * @param taskId the taskId to send the new tuple to
     * @param anchosr the tuples to anchor to
     * @param tuple the new output tuple from this bolt
     */
    public void emitDirect(int taskId, List<Tuple> anchors, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }

    /**
     * Emits a tuple directly to the specified task id on the default stream.
     * If the target bolt does not subscribe to this bolt using a direct grouping,
     * the tuple will not be sent. If the specified output stream is not declared
     * as direct, or the target bolt subscribes with a non-direct grouping,
     * an error will occur at runtime.
     *
     * <p>The default stream must be declared as direct in the topology definition.
     * See OutputDeclarer#declare for how this is done when defining topologies
     * in Java.</p>
     *
     * @param taskId the taskId to send the new tuple to
     * @param anchor the tuple to anchor to
     * @param tuple the new output tuple from this bolt
     */
    public void emitDirect(int taskId, Tuple anchor, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchor, tuple);
    }


    /**
     * Emits a tuple directly to the specified task id on the default stream.
     * If the target bolt does not subscribe to this bolt using a direct grouping,
     * the tuple will not be sent. If the specified output stream is not declared
     * as direct, or the target bolt subscribes with a non-direct grouping,
     * an error will occur at runtime.
     *
     * <p>The default stream must be declared as direct in the topology definition.
     * See OutputDeclarer#declare for how this is done when defining topologies
     * in Java.</p>
     *
     * <p>Note that this method does not use anchors, so downstream failures won't
     * affect the failure status of any spout tuples.</p>
     *
     * @param taskId the taskId to send the new tuple to
     * @param tuple the new output tuple from this bolt
     */
    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }
}
