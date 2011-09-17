package backtype.storm.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implement this interface to provide Storm with the ability to serialize and
 * deserialize types besides the primitives that you want to put into tuples.
 *
 * <p>You must declare these new serializers in the configuration of either the cluster
 * or the topology that will use these seralizations. Once you declare a serialization,
 * Storm will make use of it automatically. See "topology.serializations" in
 * {@link backtype.storm.Config} for more details.</p>
 */
public interface ISerialization<T> {
    /**
     * Returns whether this serialization can handle the given type.
     */
    public boolean accept(Class<T> c);

    /**
     * Serializes the provided object into the stream.
     */
    public void serialize(T object, DataOutputStream stream) throws IOException;

    /**
     * Reads an object of the proper type off of the stream.
     */
    public T deserialize(DataInputStream stream) throws IOException;
}