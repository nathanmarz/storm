package storm.kafka;

import java.io.Serializable;
import java.util.List;


public class SpoutConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;
    
    public SpoutConfig(String zkRoot, String id) {
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
