package backtype.storm.networktopography;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It returns the DEFAULT_RACK for every host.
 */
public final class DefaultRackDNSToSwitchMapping extends AbstractDNSToSwitchMapping {

    private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

    @Override
    public Map<String,String> resolve(List<String> names) {

        Map<String, String> m = new HashMap<String, String>();
        if (names.isEmpty()) {
            //name list is empty, return an empty map
            return m;
        }
        for (String name : names) {
            m.put(name, DEFAULT_RACK);
            mappingCache.put(name, DEFAULT_RACK);
        }
        return m;
    }

    @Override
    public String toString() {
        return "DefaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)" + dumpTopology();
    }
}
