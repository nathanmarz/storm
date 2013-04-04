package backtype.storm.security.auth.authorizer;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that denies everything, for testing purposes
 */
public class DenyAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(DenyAuthorizer.class);
    private Vector<String> enabled_ops;
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    public void prepare(Map conf) {        
        enabled_ops = new Vector<String>();

        String spec = (String)conf.get("storm.denyauthorizer.exceptions");
        if (spec == null) return;
        
        StringTokenizer tokens = new StringTokenizer(spec);
        while (tokens.hasMoreElements())  {
            String op = (String) tokens.nextElement();
            enabled_ops.add(op);
            LOG.info("Operation "+op+" will be allowed");
        }
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param contrext request context 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        LOG.info("[req "+ context.requestID()+ "] Access "
                + " from: " + (context.remoteAddress() == null? "null" : context.remoteAddress().toString())
                + (context.principal() == null? "" : (" principal:"+ context.principal()))
                +" op:"+operation
                + (topology_conf == null? "" : (" topoology:"+topology_conf.get(Config.TOPOLOGY_NAME))));
        
        if (enabled_ops.contains(operation)) return true;
        return false;
    }
}
