package storm.trident.state;

import java.io.Serializable;


public class StateSpec implements Serializable {
    public StateFactory stateFactory;
    public Integer requiredNumPartitions = null;
    
    public StateSpec(StateFactory stateFactory) {
        this.stateFactory = stateFactory;
    }
}
