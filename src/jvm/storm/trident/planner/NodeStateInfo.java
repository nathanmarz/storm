package storm.trident.planner;

import java.io.Serializable;
import storm.trident.state.StateSpec;

public class NodeStateInfo implements Serializable {
    public String id;
    public StateSpec spec;
    
    public NodeStateInfo(String id, StateSpec spec) {
        this.id = id;
        this.spec = spec;
    }
}
