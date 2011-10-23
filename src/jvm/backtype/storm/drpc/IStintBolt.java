package backtype.storm.drpc;

import backtype.storm.drpc.CoordinatedBolt.FinishedCallback;
import backtype.storm.topology.IBasicBolt;

public interface IStintBolt extends IBasicBolt, FinishedCallback {
    
}
