package storm.trident.topology;

import storm.trident.spout.IBatchID;


public class BatchInfo {
    public IBatchID batchId;
    public Object state;
    public String batchGroup;
    
    public BatchInfo(String batchGroup, IBatchID batchId, Object state) {
        this.batchGroup = batchGroup;
        this.batchId = batchId;
        this.state = state;
    }
}
