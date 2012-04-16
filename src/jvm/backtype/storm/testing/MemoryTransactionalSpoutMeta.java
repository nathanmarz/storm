package backtype.storm.testing;

public class MemoryTransactionalSpoutMeta {
    int index;
    int amt;
    
    // for kryo compatibility
    public MemoryTransactionalSpoutMeta() {
        
    }
    
    public MemoryTransactionalSpoutMeta(int index, int amt) {
        this.index = index;
        this.amt = amt;
    }

    @Override
    public String toString() {
        return "index: " + index + "; amt: " + amt;
    }    
}
