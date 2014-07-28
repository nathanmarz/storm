package backtype.storm.serialization;

import java.util.Map;

/**
 * Allow {@link backtype.storm.utils.Utils} to delegate meta serialization.
 * @author danehammer
 */
public interface SerializationDelegate {

    /**
     * Lifecycle step that will be called after instantiating with nullary constructor.
     */
    void prepare(Map stormConf);

    byte[] serialize(Object object);

    Object deserialize(byte[] bytes);
}
