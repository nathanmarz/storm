package backtype.storm.topology.base;

import backtype.storm.topology.IComponent;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseComponent implements IComponent {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }    
}
