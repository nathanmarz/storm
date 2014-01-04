package backtype.storm.drpc;

import com.google.common.net.HostAndPort;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class FixedSizeDRPCInvocationsClientPoolFactory implements Serializable, DRPCInvocationsFactory {

    private final ConcurrentHashMap<HostAndPort, FixedSizeDRPCInvocationsClientPool> serverMap =
            new ConcurrentHashMap<HostAndPort, FixedSizeDRPCInvocationsClientPool>();

    private final boolean connectImmediately;
    private final int connections;

    public FixedSizeDRPCInvocationsClientPoolFactory(int connections, boolean connectImmediately) {
        this.connectImmediately = connectImmediately;
        this.connections = connections;
    }

    @Override
    public DRPCInvocations getClientForServer(HostAndPort hostAndPort) {
        FixedSizeDRPCInvocationsClientPool pool = serverMap.get(hostAndPort);
        if (pool == null) {
            pool = new FixedSizeDRPCInvocationsClientPool(hostAndPort, connections, connectImmediately);
            FixedSizeDRPCInvocationsClientPool ret = serverMap.putIfAbsent(hostAndPort, pool);
            if (ret != null) {
                pool.close();
                pool = ret;
            }
        }
        return pool;
    }

}
