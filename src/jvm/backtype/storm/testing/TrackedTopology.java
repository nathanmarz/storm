package backtype.storm.testing;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.generated.StormTopology;
import clojure.lang.Keyword;

public class TrackedTopology extends HashMap{
	public TrackedTopology(Map map) {
		super(map);
	}
	
	public StormTopology getTopology() {
		return (StormTopology)get(Keyword.intern("topology"));
	}
}
