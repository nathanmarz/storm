/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package backtype.storm.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class JavaSerialization implements ISerialization<Serializable> {
    public boolean accept(Class c) {
        return Serializable.class.isAssignableFrom(c);
    }

    public void serialize(Serializable object, DataOutputStream stream) throws IOException {
        new ObjectOutputStream(stream).writeObject(object);
    }

    public Serializable deserialize(DataInputStream stream) throws IOException {
        try {
            return (Serializable) new ObjectInputStream(stream).readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
