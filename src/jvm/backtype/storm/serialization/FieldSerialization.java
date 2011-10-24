package backtype.storm.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class FieldSerialization {
    private int _token;
    private ISerialization _serialization;

    public FieldSerialization(int token, ISerialization serialization) {
        _serialization = serialization;
        _token = token;
    }

    public int getToken() {
        return _token;
    }

    public ISerialization getSerialization() {
        return _serialization;
    }    
    
    public void serialize(Object obj, DataOutputStream out) throws IOException {
        _serialization.serialize(obj, out);
    }

    public Object deserialize(DataInputStream in) throws IOException {
        return _serialization.deserialize(in);
    }

}
