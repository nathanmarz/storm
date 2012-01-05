package backtype.storm;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.DistributedRPCInvocations;


public interface ILocalDRPC extends DistributedRPC.Iface, DistributedRPCInvocations.Iface, Shutdownable {
    public String getServiceId();    
}
