package backtype.storm.utils;

public class MutableInt {
    int val;

    public MutableInt(int val) {
        this.val = val;
    }
    
    public void set(int val) {
        this.val = val;
    }
    
    public int get() {
        return val;
    }
    
    public int increment() {
        return increment(1);
    }
    
    public int increment(int amt) {
        val+=amt;
        return val;
    }
}
