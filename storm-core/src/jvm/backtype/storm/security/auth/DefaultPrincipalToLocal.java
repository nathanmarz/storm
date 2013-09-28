package backtype.storm.security.auth;

import java.util.Map;
import java.security.Principal;

/**
 * Storm can be configured to launch worker processed as a given user.
 * Some transports need to map the Principal to a local user name.
 */
public class DefaultPrincipalToLocal implements IPrincipalToLocal {
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    public void prepare(Map storm_conf) {}
    
    /**
     * Convert a Principal to a local user name.
     * @param principal the principal to convert
     * @return The local user name.
     */
    public String toLocal(Principal principal) {
      return principal == null ? null : principal.getName();
    }
}
