package backtype.storm.hooks.info;

public class SpoutFailInfo {
    public Object messageId;
    public long failLatencyMs;
    
    public SpoutFailInfo(Object messageId, long failLatencyMs) {
        this.messageId = messageId;
        this.failLatencyMs = failLatencyMs;
    }
}
