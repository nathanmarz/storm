package backtype.storm.drpc;

import com.google.common.net.HostAndPort;

import java.io.Serializable;

public interface DRPCInvocationsFactory extends Serializable {

    DRPCInvocations getClientForServer(HostAndPort hostAndPort);

}
