package org.apache.storm.utils;

import org.apache.storm.cluster.ConnectionState;

import java.util.HashMap;
import java.util.Map;

public class StormConnectionStateConverter {

    private static final Map<org.apache.curator.framework.state.ConnectionState, ConnectionState> mapCuratorToStorm = new HashMap<>();
    static {
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.CONNECTED, ConnectionState.CONNECTED);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.LOST, ConnectionState.LOST);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.RECONNECTED, ConnectionState.RECONNECTED);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.READ_ONLY, ConnectionState.LOST);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.SUSPENDED, ConnectionState.LOST);
    }

    public static ConnectionState convert(org.apache.curator.framework.state.ConnectionState state) {
        ConnectionState stormState = mapCuratorToStorm.get(state);
        if (stormState != null) {
            return stormState;
        }
        throw new IllegalStateException("Unknown ConnectionState from Curator: " + state);
    }
}
