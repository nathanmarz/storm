package backtype.storm.security.auth;

import com.google.common.annotations.VisibleForTesting;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import javax.security.auth.Subject;

import backtype.storm.scheduler.TopologyDetails;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.InetAddress;

/**
 * context request context includes info about 
 *      		   (1) remote address/subject, 
 *                 (2) operation
 *                 (3) configuration of targeted topology 
 */
public class ReqContext {
    private static final AtomicInteger uniqueId = new AtomicInteger(0);

    public enum OperationType { SUBMIT_TOPOLOGY, KILL_TOPOLOGY, REBALANCE_TOPOLOGY, ACTIVATE_TOPOLOGY, DEACTIVATE_TOPOLOGY }; 
    
    private Subject _subject;
    private InetAddress _remoteAddr;
    private Integer _reqID;
    private Map _storm_conf;
	private OperationType _operation;
    
    /**
     * Get a request context associated with current thread
     * @return
     */
    public static ReqContext context() {
		return ctxt.get();
	}

    //each thread will have its own request context
    private static final ThreadLocal < ReqContext > ctxt = 
        new ThreadLocal < ReqContext > () {
            @Override protected ReqContext initialValue() {
                return new ReqContext(AccessController.getContext());
        }
    };

    //private constructor
	@VisibleForTesting
    ReqContext(AccessControlContext acl_ctxt) {
		_subject = Subject.getSubject(acl_ctxt);
		_reqID = uniqueId.incrementAndGet();
    }
    
    /**
     * client address
     */
    public void setRemoteAddress(InetAddress addr) {
    	_remoteAddr = addr;
    }
    
    public InetAddress remoteAddress() {
    	return _remoteAddr;
    }
    
    /**
     * Set remote subject explicitly
     */
    public void setSubject(Subject subject) {
    	_subject = subject;	
    }
    
    /**
     * Client subject associated with this request context
     * @return
     */
    public Subject subject() {
		return _subject;
	}

    /**
     * The primary principal associated current subject
     * @return
     */
    public Principal principal() {
		if (_subject == null) return null;
		Set<Principal> princs = _subject.getPrincipals();
		if (princs.size()==0) return null;
		return (Principal) (princs.toArray()[0]);
	}
    
    /**
     * Topology that this request is against
     */
    public Map topologyConf() {
    	return _storm_conf;
    }
    
    public void setTopologyConf(Map conf) {
    	_storm_conf = conf;
    }
    
    /**
     * Operation that this request is performing
     */
    public OperationType operation() {
    	return _operation;
    }
    
    public void setOperation(OperationType operation) {
    	_operation = operation;
    }
}
