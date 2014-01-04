package backtype.storm.drpc;

import backtype.storm.generated.DistributedRPCInvocations;

public interface DRPCInvocations extends DistributedRPCInvocations.Iface {

    void close();

    int getPort();

    String getHost();

}
