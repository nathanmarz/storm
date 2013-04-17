package backtype.storm.hooks.info;

public class SpoutAckInfo {
    public Object messageId;
    public int spoutTaskId;
    public Long completeLatencyMs; // null if it wasn't sampled
    
    public SpoutAckInfo(Object messageId, int spoutTaskId, Long completeLatencyMs) {
        this.messageId = messageId;
        this.spoutTaskId = spoutTaskId;
        this.completeLatencyMs = completeLatencyMs;
    }
}
