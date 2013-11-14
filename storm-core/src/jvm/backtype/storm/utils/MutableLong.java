package backtype.storm.utils;

public class MutableLong {
    long val;

    public MutableLong(long val) {
        this.val = val;
    }
    
    public void set(long val) {
        this.val = val;
    }
    
    public long get() {
        return val;
    }
    
    public long increment() {
        return increment(1);
    }
    
    public long increment(long amt) {
        val+=amt;
        return val;
    }
}
