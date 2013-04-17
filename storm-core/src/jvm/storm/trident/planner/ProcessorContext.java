package storm.trident.planner;


public class ProcessorContext {
    public Object batchId;
    public Object[] state;
    
    public ProcessorContext(Object batchId, Object[] state) {
        this.batchId = batchId;
        this.state = state;
    }
}
