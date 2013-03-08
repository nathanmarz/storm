package backtype.storm.security.auth.authorizer;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A no-op authorization implementation that illustrate info available for authorization decisions.
 */
public class NoopAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(NoopAuthorizer.class);

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
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        LOG.info("[req "+ context.requestID()+ "] Access "
                + " from: " + (context.remoteAddress() == null? "null" : context.remoteAddress().toString())
                + " principal:"+(context.principal() == null? "null" : context.principal()) 
                +" op:"+ operation
                + " topoology:"+ topology_conf.get(Config.TOPOLOGY_NAME));
        return true;
    }
}
