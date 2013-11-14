package backtype.storm.topology;

import java.io.Serializable;
import java.util.Map;

/**
 * Common methods for all possible components in a topology. This interface is used
 * when defining topologies using the Java API. 
 */
public interface IComponent extends Serializable {

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);

    /**
     * Declare configuration specific to this component. Only a subset of the "topology.*" configs can
     * be overridden. The component configuration can be further overridden when constructing the 
     * topology using {@link TopologyBuilder}
     *
     */
    Map<String, Object> getComponentConfiguration();

}
