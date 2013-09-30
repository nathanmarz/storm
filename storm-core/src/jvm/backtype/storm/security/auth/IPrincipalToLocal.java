package backtype.storm.security.auth;

import java.util.Map;
import java.security.Principal;

/**
 * Some transports need to map the Principal to a local user name.
 */
public interface IPrincipalToLocal {
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    void prepare(Map storm_conf);
    
    /**
     * Convert a Principal to a local user name.
     * @param principal the principal to convert
     * @return The local user name.
     */
    public String toLocal(Principal principal);
}
