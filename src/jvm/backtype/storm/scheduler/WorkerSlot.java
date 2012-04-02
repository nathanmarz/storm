package backtype.storm.scheduler;

public class WorkerSlot {
    String nodeId;
    int port;
    
    public WorkerSlot(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
    }
}
