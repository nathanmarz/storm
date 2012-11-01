package backtype.storm.hooks;

import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.task.TopologyContext;
import java.util.Map;

public interface ITaskHook {
    void prepare(Map conf, TopologyContext context);
    void cleanup();
    void emit(EmitInfo info);
    void spoutAck(SpoutAckInfo info);
    void spoutFail(SpoutFailInfo info);
    void boltExecute(BoltExecuteInfo info);
    void boltAck(BoltAckInfo info);
    void boltFail(BoltFailInfo info);
}
