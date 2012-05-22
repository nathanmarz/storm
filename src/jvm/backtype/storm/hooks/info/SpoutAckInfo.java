package backtype.storm.hooks.info;

public class SpoutAckInfo {
    public Object messageId;
    public Long completeLatencyMs; // null if it wasn't sampled
    
    public SpoutAckInfo(Object messageId, Long completeLatencyMs) {
        this.messageId = messageId;
        this.completeLatencyMs = completeLatencyMs;
    }
}
