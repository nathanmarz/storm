package backtype.storm;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.generated.DRPCExecutionException;

import org.apache.thrift7.TException;
import java.nio.charset.CharacterCodingException;

public interface ILocalDRPC extends DistributedRPC.Iface, DistributedRPCInvocations.Iface, Shutdownable {
    public String getServiceId();    

    /**
    * Overloaded execute method for backwards compatibility
    **/
    // public String execute(String func, String args) throws TException, DRPCExecutionException, CharacterCodingException;
}
