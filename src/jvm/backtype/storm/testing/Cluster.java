package backtype.storm.testing;

import java.util.HashMap;
import java.util.Map;

import clojure.lang.Keyword;

public class Cluster extends HashMap {
	public Cluster(Map clusterMap) {
		super(clusterMap);
	}
	
	public Object getNimbus() {
		return get(Keyword.intern("nimbus"));
	}
}
