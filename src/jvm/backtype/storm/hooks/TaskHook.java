package backtype.storm.hooks;

import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.BoltFailInfo;

public interface TaskHook {
    void emit(EmitInfo info);
    void spoutAck(SpoutAckInfo info);
    void spoutFail(SpoutFailInfo info);
    void boltAck(BoltAckInfo info);
    void boltFail(BoltFailInfo info);
}
