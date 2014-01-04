package backtype.storm.drpc;

import com.google.common.net.HostAndPort;

public class DRPCInvocationsClientFactory implements DRPCInvocationsFactory {

    @Override
    public DRPCInvocations getClientForServer(HostAndPort hostAndPort) {
        return new DRPCInvocationsClient(hostAndPort.getHostText(), hostAndPort.getPort());
    }

}
