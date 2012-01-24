package backtype.storm.testing;

public class MemoryTransactionalSpoutMeta {
    int index;
    int amt;

    public MemoryTransactionalSpoutMeta(int index, int amt) {
        this.index = index;
        this.amt = amt;
    }    
}
