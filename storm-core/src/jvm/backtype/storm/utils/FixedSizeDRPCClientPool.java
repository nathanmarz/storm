package backtype.storm.utils;

import backtype.storm.drpc.AbstractFixedSizePool;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import com.google.common.net.HostAndPort;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FixedSizeDRPCClientPool extends AbstractFixedSizePool<FixedSizeDRPCClientPool.RetryConnectionOnceDRPCClient> implements DistributedRPC.Iface {

    public static Logger LOG = LoggerFactory.getLogger(FixedSizeDRPCClientPool.class);

    public FixedSizeDRPCClientPool(List<HostAndPort> hosts, int connectionsPerHost, int timeout, boolean connectImmediately) {
        super(connectionsPerHost * hosts.size());
        List<RetryConnectionOnceDRPCClient> clients = new ArrayList<RetryConnectionOnceDRPCClient>(connectionsPerHost * hosts.size());
        for (HostAndPort host : hosts) {
            for (int i = 0; i < connectionsPerHost; ++i)
                clients.add(createClient(timeout, connectImmediately, host));
        }
        // Shuffle clients so load is "uniformly" distributed
        Collections.shuffle(clients);
        queue.addAll(clients);
    }

    private RetryConnectionOnceDRPCClient createClient(int timeout, boolean connectImmediately, HostAndPort host) {
        RetryConnectionOnceDRPCClient client = new RetryConnectionOnceDRPCClient(host.getHostText(), host.getPort(), timeout, false);
        // We don't want the DRPC client's being unavailable to stop client startup, so connect in a separate step.
        if (connectImmediately) {
            try {
                client.ensureConnected();
            } catch (TException e) {
                LOG.warn("Unable to eagerly connect to {}, leaving in unconnected state: ", host, e);
                client.close();
            }
        }
        return client;

    }

    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, TException {
        RetryConnectionOnceDRPCClient client = take();
        try {
            return client.execute(functionName, funcArgs);
        } finally {
            put(client);
        }
    }

    public String execute(String functionName, String funcArgs, int timeout, TimeUnit timeUnit) throws DRPCExecutionException, TException {
        RetryConnectionOnceDRPCClient client = take(timeout, timeUnit);
        if (client == null)
            return null;
        try {
            return client.execute(functionName, funcArgs);
        } finally {
            put(client);
        }
    }

    public static class RetryConnectionOnceDRPCClient extends DRPCClient {

        public RetryConnectionOnceDRPCClient(String host, int port, Integer timeout, boolean connectImmediately) {
            super(host, port, timeout, connectImmediately);
        }

        @Override
        public String doExecute(String func, String args) throws TException, DRPCExecutionException {
            try {
                return super.doExecute(func, args);
            } catch (TTransportException e) {
                // We'll catch the first transport-related connection (timeout, reconnect, whatever) and retry
                LOG.debug("Caught transport exception during execution; reconnecting and re-trying: ", e);
                close();
                return super.doExecute(func, args);
            }
        }
    }
}
