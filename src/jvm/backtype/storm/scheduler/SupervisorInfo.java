package backtype.storm.scheduler;

public class SupervisorInfo {
    String id;
    Object meta;
    
    public SupervisorInfo(String id, Object meta) {
        this.id = id;
        this.meta = meta;
    }
    
    public String getId() {
        return id;
    }
    
    public Object getMeta() {
        return meta;
    }
}
