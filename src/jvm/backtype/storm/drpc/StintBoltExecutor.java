package backtype.storm.drpc;

import backtype.storm.drpc.CoordinatedBolt.FinishedCallback;
import backtype.storm.topology.BasicBoltExecutor;

public class StintBoltExecutor extends BasicBoltExecutor implements FinishedCallback {
    IStintBolt _bolt;
    
    public StintBoltExecutor(IStintBolt bolt) {
        super(bolt);
        _bolt = bolt;
    }

    @Override
    public void finishedId(Object id) {
        _bolt.finishedId(_bolt);
    }
}
