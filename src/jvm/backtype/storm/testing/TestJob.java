package backtype.storm.testing;

import java.util.Map;

public interface TestJob {
    public Object run(Map clusterMap);
}
