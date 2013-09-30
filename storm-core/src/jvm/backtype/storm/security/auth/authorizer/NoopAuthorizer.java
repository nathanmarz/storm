package backtype.storm.security.auth.authorizer;

import java.util.Map;

import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

/**
 * A no-op authorization implementation that illustrate info available for authorization decisions.
 */
public class NoopAuthorizer implements IAuthorizer {
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    public void prepare(Map conf) {        
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_conf configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        return true;
    }
}
