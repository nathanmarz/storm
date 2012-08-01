package storm.trident.topology;


public class BatchInfo {
    public Object batchId;
    public Object state;
    public String batchGroup;
    
    public BatchInfo(String batchGroup, Object batchId, Object state) {
        this.batchGroup = batchGroup;
        this.batchId = batchId;
        this.state = state;
    }
}
