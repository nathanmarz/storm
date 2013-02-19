package backtype.storm.security.auth;

import backtype.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that denies everything, for testing purposes
 */
public class DenyAuthorizer implements IAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(DenyAuthorizer.class);

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param contrext request context includes info about 
     *             (1) remote address/subject, 
     *             (2) operation
     *             (3) configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context) {
        LOG.info("Access "
                + " from: " + (context.remoteAddress() == null? "null" : context.remoteAddress().toString())
                + " principal:"+ (context.principal() == null? "null" : context.principal())
                +" op:"+context.operation()
                + " topoology:"+context.topologyConf().get(Config.TOPOLOGY_NAME));
        return false;
    }
}
