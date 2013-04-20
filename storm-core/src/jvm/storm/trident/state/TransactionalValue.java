package storm.trident.state;

import org.apache.commons.lang.builder.ToStringBuilder;


public class TransactionalValue<T> {
    T val;
    Long txid;
    
    public TransactionalValue(Long txid, T val) {
        this.val = val;
        this.txid = txid;
    }
    
    public T getVal() {
        return val;
    }
    
    public Long getTxid() {
        return txid;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
