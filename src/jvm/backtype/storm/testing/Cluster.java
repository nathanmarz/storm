package backtype.storm.testing;

import java.util.HashMap;
import java.util.Map;

import clojure.lang.Keyword;

/**
 * This is the <code>cluster</code> which <code>Testing.withSimulatedTimeLocalCluster</code>
 * and <code>Testing.withTrackedCluster</code> will give us. It is actually a map, but we define
 * a class to give developer a strong type. Developer don't have to care what are in this map, usually
 * we just pass the cluster to <code>completeTopology</code> or <code>mkTrackedTopology</code>.
 */
public class Cluster extends HashMap {
	public Cluster(Map clusterMap) {
		super(clusterMap);
	}
	
	/**
	 * Return the nimbus(<code>backtype.storm.daemon.nimbus</code>).
	 * 
	 * Why dont return an <code>Object</code> instead of <code>backtype.storm.daemon.nimbus</code>? 
	 * Because <code>backtype.storm.daemon.nimbus</code> is defined in clojure, if we return 
	 * <code>backtype.storm.daemon.nimbus</code>, it makes java code depends on clojure code, while
	 * clojure code already depends on the interfaces defined in clojure, it introduces a circular dependencies.
	 * 
	 * @return the nimbus
	 */
	public Object getNimbus() {
		return get(Keyword.intern("nimbus"));
	}
}
