package backtype.storm;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DistributedRPC;


public interface ILocalDRPC extends DistributedRPC.Iface, Shutdownable {
    public String getServiceId();    
}
