package backtype.storm.nimbus;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import java.util.Map;

public class DefaultTopologyValidator implements ITopologyValidator {
    @Override
    public void validate(String topologyName, Map topologyConf, StormTopology topology) throws InvalidTopologyException {        
    }    
}
