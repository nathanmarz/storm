package backtype.storm.drpc;

import backtype.storm.ILocalDRPC;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.utils.ServiceRegistry;
import com.google.common.net.HostAndPort;
import org.apache.thrift7.TException;

public class LocalDRPCInvocationFactory implements DRPCInvocationsFactory {

    protected final String localDrpcId;

    public LocalDRPCInvocationFactory(ILocalDRPC drpc) {
        localDrpcId = drpc.getServiceId();
    }

    @Override
    public DRPCInvocations getClientForServer(HostAndPort hostAndPort) {
        return new LocalDRPCInvocationClient((DRPCInvocations) ServiceRegistry.getService(localDrpcId), hostAndPort);
    }

    private static class LocalDRPCInvocationClient implements DRPCInvocations {

        final DistributedRPCInvocations.Iface delegate;
        final HostAndPort hostAndPort;

        private LocalDRPCInvocationClient(DRPCInvocations delegate, HostAndPort hostAndPort) {
            this.delegate = delegate;
            this.hostAndPort = hostAndPort;
        }

        @Override
        public void close() {
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
