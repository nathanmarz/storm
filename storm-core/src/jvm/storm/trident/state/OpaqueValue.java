package storm.trident.state;

import org.apache.commons.lang.builder.ToStringBuilder;

public class OpaqueValue<T> {
    Long currTxid;
    T prev;
    T curr;
    
    public OpaqueValue(Long currTxid, T val, T prev) {
        this.curr = val;
        this.currTxid = currTxid;
        this.prev = prev;
    }

    public OpaqueValue(Long currTxid, T val) {
        this(currTxid, val, null);
    }
    
    public OpaqueValue<T> update(Long batchTxid, T newVal) {
        T prev;
        if(batchTxid==null || (this.currTxid < batchTxid)) {
            prev = this.curr;
        } else if(batchTxid.equals(this.currTxid)){
            prev = this.prev;
        } else {
            throw new RuntimeException("Current batch (" + batchTxid + ") is behind state's batch: " + this.toString());
        }
        return new OpaqueValue<T>(batchTxid, newVal, prev);
    }
    
    public T get(Long batchTxid) {
        if(batchTxid==null || (this.currTxid < batchTxid)) {
            return curr;
        } else if(batchTxid.equals(this.currTxid)){
            return prev;
        } else {
            throw new RuntimeException("Current batch (" + batchTxid + ") is behind state's batch: " + this.toString());
        }
    }
    
    public T getCurr() {
        return curr;
    }
    
    public Long getCurrTxid() {
        return currTxid;
    }
    
    public T getPrev() {
        return prev;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
