
package backtype.storm.testing;

import backtype.storm.serialization.ISerialization;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TestSerObjectSerialization implements ISerialization<TestSerObject> {

    @Override
    public boolean accept(Class c) {
        return c.equals(TestSerObject.class);
    }

    @Override
    public void serialize(TestSerObject object, DataOutputStream stream) throws IOException {
        stream.writeInt(object.f1);
        stream.writeInt(object.f2);
    }

    @Override
    public TestSerObject deserialize(DataInputStream stream) throws IOException {
        int f1 = stream.readInt();
        int f2 = stream.readInt();
        return new TestSerObject(f1, f2);
    }    
}
