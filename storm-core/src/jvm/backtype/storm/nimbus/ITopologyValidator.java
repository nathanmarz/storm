package backtype.storm.nimbus;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import java.util.Map;

public interface ITopologyValidator {
    void prepare(Map StormConf);
    void validate(String topologyName, Map topologyConf, StormTopology topology, Map NimbusConf)
            throws InvalidTopologyException;
}
