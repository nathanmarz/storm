package org.apache.storm.flux.api;


import backtype.storm.generated.StormTopology;

import java.util.Map;

/**
 * Marker interface for objects that can produce `StormTopology` objects.
 *
 * If a `topology-source` class implements the `getTopology()` method, Flux will
 * call that method. Otherwise, it will introspect the given class and look for a
 * similar method that produces a `StormTopology` instance.
 *
 * Note that it is not strictly necessary for a class to implement this interface.
 * If a class defines a method with a similar signature, Flux should be able to find
 * and invoke it.
 *
 */
public interface TopologySource {
    public StormTopology getTopology(Map<String, Object> config);
}
