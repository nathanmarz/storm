package backtype.storm.testing;

import java.io.Serializable;

public class TestSerObject implements Serializable {
    public int f1;
    public int f2;
    
    public TestSerObject(int f1, int f2) {
       this.f1 = f1;
       this.f2 = f2;
    }

    @Override
    public boolean equals(Object o) {
        TestSerObject other = (TestSerObject) o;
        return f1 == other.f1 && f2 == other.f2;
    }
        
}
