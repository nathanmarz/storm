package backtype.storm.security.auth;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.InetAddress;
import com.google.common.annotations.VisibleForTesting;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import javax.security.auth.Subject;

/**
 * context request context includes info about 
 *      	   (1) remote address, 
 *             (2) remote subject and primary principal
 *             (3) request ID 
 */
public class ReqContext {
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    private Subject _subject;
    private InetAddress _remoteAddr;
    private Integer _reqID;
    private Map _storm_conf;

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
        @Override 
        protected ReqContext initialValue() {
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
     * Retrieve client subject associated with this request context
     */
    public Subject subject() {
        return _subject;
    }

    /**
     * The primary principal associated current subject
     */
    public Principal principal() {
        if (_subject == null) return null;
        Set<Principal> princs = _subject.getPrincipals();
        if (princs.size()==0) return null;
        return (Principal) (princs.toArray()[0]);
    }
    
    /**
     * request ID of this request
     */
    public Integer requestID() {
        return _reqID;
    }
}
