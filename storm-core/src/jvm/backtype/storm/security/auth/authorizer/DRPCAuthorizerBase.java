package backtype.storm.security.auth.authorizer;

import java.util.Map;

import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DRPCAuthorizerBase implements IAuthorizer {
    public static Logger LOG = LoggerFactory.getLogger(DRPCAuthorizerBase.class);

    /**
     * A key name for the function requested to be executed by a user.
     */
    public static final String FUNCTION_NAME = "function.name";

    @Override
    public abstract void prepare(Map conf);

    abstract protected boolean permitClientRequest(ReqContext context, String operation, Map params);

    abstract protected boolean permitInvocationRequest(ReqContext context, String operation, Map params);
    
    /**
     * Authorizes request from to the DRPC server.
     * @param context the client request context
     * @param operation the operation requested by the DRPC server
     * @param params a Map with any key-value entries of use to the authorization implementation
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map params) {
        if ("execute".equals(operation)) {
            return permitClientRequest(context, operation, params);
        } else if ("failRequest".equals(operation) || 
                "fetchRequest".equals(operation) || 
                "result".equals(operation)) {
            return permitInvocationRequest(context, operation, params);
        }
        // Deny unsupported operations.
        LOG.warn("Denying unsupported operation \""+operation+"\" from "+
                context.remoteAddress());
        return false;
    }
}
