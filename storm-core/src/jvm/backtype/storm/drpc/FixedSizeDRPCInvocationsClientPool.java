package backtype.storm.drpc;

import backtype.storm.generated.DRPCRequest;
import com.google.common.net.HostAndPort;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class FixedSizeDRPCInvocationsClientPool extends AbstractFixedSizePool<FixedSizeDRPCInvocationsClientPool.RetryConnectionOnceDRPCInvocationsClient> implements DRPCInvocations {

    public static Logger LOG = LoggerFactory.getLogger(FixedSizeDRPCInvocationsClientPool.class);

    public final HostAndPort hostAndPort;

    public FixedSizeDRPCInvocationsClientPool(HostAndPort hostAndPort, int connections, boolean connectImmediately) {
        super(connections);
        this.hostAndPort = hostAndPort;
        for (int i = 0; i < connections; ++i)
            queue.add(createClient(connectImmediately));
    }

    private RetryConnectionOnceDRPCInvocationsClient createClient(boolean connectImmediately) {
        RetryConnectionOnceDRPCInvocationsClient client = new RetryConnectionOnceDRPCInvocationsClient(hostAndPort.getHostText(),
                hostAndPort.getPort(), false);
        // We don't want the DRPC client's being unavailable to stop client startup, so connect in a separate step.
        if (connectImmediately) {
            try {
                client.ensureConnected();
            } catch (TException e) {
                LOG.warn("Unable to eagerly connect to {}, leaving in unconnected state: ", hostAndPort, e);
                client.close();
            }
        }
        return client;
    }

    @Override
    public synchronized void close() {
        final int connections = queue.size();
        Set<RetryConnectionOnceDRPCInvocationsClient> set = new HashSet<RetryConnectionOnceDRPCInvocationsClient>(connections);
        while (set.size() != connections) {
            queue.drainTo(set);
        }
        for (RetryConnectionOnceDRPCInvocationsClient client : set) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Error encountered closing connection to {}, ignoring: ", hostAndPort, e);
            }
        }
        queue.addAll(set);
    }

    @Override
    public int getPort() {
        return hostAndPort.getPort();
    }

    @Override
    public String getHost() {
        return hostAndPort.getHostText();
    }

    @Override
    public void result(String id, String result) throws TException {
        RetryConnectionOnceDRPCInvocationsClient client = take();
        try {
            client.result(id, result);
        } finally {
            put(client);
        }
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws TException {
        RetryConnectionOnceDRPCInvocationsClient client = take();
        try {
            return client.fetchRequest(functionName);
        } finally {
            put(client);
        }
    }

    @Override
    public void failRequest(String id) throws TException {
        RetryConnectionOnceDRPCInvocationsClient client = take();
        try {
            client.failRequest(id);
        } finally {
            put(client);
        }
    }

    public static class RetryConnectionOnceDRPCInvocationsClient extends DRPCInvocationsClient {

        public RetryConnectionOnceDRPCInvocationsClient(String host, int port, boolean connectImmediately) {
            super(host, port, connectImmediately);
        }

        @Override
        protected void doFailRequest(String id) throws TException {
            try {
                super.doFailRequest(id);
            } catch (TTransportException e) {
                // We'll catch the first transport-related connection (timeout, reconnect, whatever) and retry
                LOG.debug("Caught transport exception failing request; reconnecting and re-trying: ", e);
                close();
                super.doFailRequest(id);
            }
        }

        @Override
        protected DRPCRequest doFetchRequest(String func) throws TException {
            try {
                return super.doFetchRequest(func);
            } catch (TTransportException e) {
                // We'll catch the first transport-related connection (timeout, reconnect, whatever) and retry
                LOG.debug("Caught transport exception during fetch; reconnecting and re-trying: ", e);
                close();
                return super.doFetchRequest(func);
            }
        }

        @Override
        protected void doResult(String id, String result) throws TException {
            try {
                super.doResult(id, result);
            } catch (TTransportException e) {
                // We'll catch the first transport-related connection (timeout, reconnect, whatever) and retry
                LOG.debug("Caught transport exception returning result; reconnecting and re-trying: ", e);
                close();
                super.doResult(id, result);
            }
        }
    }
}
