package backtype.storm.serialization;

import com.esotericsoftware.kryo.Kryo;
import java.util.Map;

/**
 * An interface that controls the Kryo instance used by Storm for serialization.
 * The lifecycle is:
 * 
 * 1. The Kryo instance is constructed using getKryo
 * 2. Storm registers the default classes (e.g. arrays, lists, maps, etc.)
 * 3. Storm calls preRegister hook
 * 4. Storm registers all user-defined registrations through topology.kryo.register
 * 5. Storm calls postRegister hook
 * 6. Storm calls all user-defined decorators through topology.kryo.decorators
 * 7. Storm calls postDecorate hook
 */
public interface IKryoFactory {
    Kryo getKryo(Map conf);
    void preRegister(Kryo k, Map conf);
    void postRegister(Kryo k, Map conf);
    void postDecorate(Kryo k, Map conf);
}