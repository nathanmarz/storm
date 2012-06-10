package backtype.storm.hooks.info;

public class SpoutFailInfo {
    public Object messageId;
    public int spoutTaskId;
    public Long failLatencyMs; // null if it wasn't sampled
    
    public SpoutFailInfo(Object messageId, int spoutTaskId, Long failLatencyMs) {
        this.messageId = messageId;
        this.spoutTaskId = spoutTaskId;
        this.failLatencyMs = failLatencyMs;
    }
}
