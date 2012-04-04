package backtype.storm.scheduler;

public class WorkerSlot {
    String nodeId;
    int port;
    Object meta = null;
    
    public WorkerSlot(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
    }
    
    public WorkerSlot(String nodeId, int port, Object meta) {
        this.nodeId = nodeId;
        this.port = port;
        this.meta = meta;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public int getPort() {
        return port;
    }
    
    public Object getMeta() {
        return meta;
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode() + 13 * ((Integer) port).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        WorkerSlot other = (WorkerSlot) o;
        return this.port == other.port && this.nodeId.equals(other.nodeId);
    }    
}
