package backtype.storm.hooks;

import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;

public class BaseTaskHook implements TaskHook {

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
    public void boltAck(BoltAckInfo info) {
    }

    @Override
    public void boltFail(BoltFailInfo info) {
    }    
}
