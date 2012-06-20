package backtype.storm.hooks;

import backtype.storm.hooks.info.bolthAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.bolthFailInfo;
import backtype.storm.task.TopologyContext;
import java.util.Map;

public interface ITaskHook {
    void prepare(Map conf, TopologyContext context);
    void cleanup();
    void emit(EmitInfo info);
    void spoutAck(SpoutAckInfo info);
    void spoutFail(SpoutFailInfo info);
    void bolthAck(bolthAckInfo info);
    void bolthFail(bolthFailInfo info);
}
