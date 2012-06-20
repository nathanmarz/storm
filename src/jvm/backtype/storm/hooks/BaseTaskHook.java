package backtype.storm.hooks;

import backtype.storm.hooks.info.bolthAckInfo;
import backtype.storm.hooks.info.bolthFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.TopologyContext;
import java.util.Map;

public class BaseTaskHook implements ITaskHook {
    @Override
    public void prepare(Map conf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    

    @Override
    public void emit(EmitInfo info) {
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {
    }

    @Override
    public void bolthAck(bolthAckInfo info) {
    }

    @Override
    public void bolthFail(bolthFailInfo info) {
    }
}
