package backtype.storm.utils;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for retrieving fields from a {@link StormTopology}.
 */
public class ThriftTopologyUtils {

    /**
     * Fields we care about in both methods' use-cases
     */
    private static final StormTopology._Fields[] COMPONENT_FIELDS = new StormTopology._Fields[] {StormTopology._Fields.BOLTS, StormTopology._Fields.SPOUTS, StormTopology._Fields.STATE_SPOUTS };

    /**
     * Returns a set of the component IDs defined across the spouts, bolts, and state_spouts fields on the given {@code topology}.
     */
    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<String>();
        for(StormTopology._Fields f: COMPONENT_FIELDS) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            ret.addAll(componentMap.keySet());
        }
        return ret;
    }

    /**
     * Gets the ComponentCommon object mapped to the given {@code componentId} from the given {@code topology}.
     */
    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        for(StormTopology._Fields f: COMPONENT_FIELDS) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            if(componentMap.containsKey(componentId)) {
                Object component = componentMap.get(componentId);
                if(component instanceof Bolt) {
                    return ((Bolt) component).get_common();
                }
                if(component instanceof SpoutSpec) {
                    return ((SpoutSpec) component).get_common();
                }
                if(component instanceof StateSpoutSpec) {
                    return ((StateSpoutSpec) component).get_common();
                }
                throw new RuntimeException("Unreachable code! No get_common conversion for component " + component);
            }
        }
        throw new IllegalArgumentException("Could not find component common for " + componentId);
    }
}
