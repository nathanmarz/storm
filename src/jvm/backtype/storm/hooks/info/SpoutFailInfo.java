package backtype.storm.hooks.info;

public class SpoutFailInfo {
    public Object messageId;
    public Long failLatencyMs; // null if it wasn't sampled
    
    public SpoutFailInfo(Object messageId, Long failLatencyMs) {
        this.messageId = messageId;
        this.failLatencyMs = failLatencyMs;
    }
}
