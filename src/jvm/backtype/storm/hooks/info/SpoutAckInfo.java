package backtype.storm.hooks.info;

public class SpoutAckInfo {
    public Object messageId;
    public long completeLatencyMs;
    
    public SpoutAckInfo(Object messageId, long completeLatencyMs) {
        this.messageId = messageId;
        this.completeLatencyMs = completeLatencyMs;
    }
}
