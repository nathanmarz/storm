package storm.trident.state;

import java.io.Serializable;


public interface Serializer<T> extends Serializable {
    byte[] serialize(T obj);
    T deserialize(byte[] b);
}
