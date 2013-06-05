package backtype.storm.drpc;

import backtype.storm.ILocalDRPC;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.utils.ServiceRegistry;
import com.google.common.net.HostAndPort;
import org.apache.thrift7.TException;

import static org.apache.commons.lang.StringUtils.isBlank;

public class LocalDRPCInvocationFactory implements DRPCInvocationsFactory {

    protected final String localDrpcId;

    public LocalDRPCInvocationFactory() {
        localDrpcId = null;
    }

    public LocalDRPCInvocationFactory(ILocalDRPC drpc) {
        localDrpcId = drpc.getServiceId();
    }

    @Override
    public DRPCInvocations getClientForServer(HostAndPort hostAndPort) {
        String host = hostAndPort.getHostText();
        if (!isBlank(localDrpcId) && !localDrpcId.equals(host)) {
            throw new IllegalStateException("Mismatched local drpc ids: " + localDrpcId + ", " + hostAndPort);
        }
        return new LocalDRPCInvocationClient((DRPCInvocations) ServiceRegistry.getService(host), host);
    }

    private static class LocalDRPCInvocationClient implements DRPCInvocations {

        final DistributedRPCInvocations.Iface delegate;
        final String localDrpcId;

        private LocalDRPCInvocationClient(DRPCInvocations delegate, String localDrpcId) {
            this.delegate = delegate;
            this.localDrpcId = localDrpcId;
        }

        @Override
        public void close() {
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public String getHost() {
            return localDrpcId;
        }

        @Override
        public void result(String id, String result) throws TException {
                delegate.result(id, result);
        }

        @Override
        public DRPCRequest fetchRequest(String functionName) throws TException {
            return delegate.fetchRequest(functionName);
        }

        @Override
        public void failRequest(String id) throws TException {
                delegate.failRequest(id);
        }
    }
}
