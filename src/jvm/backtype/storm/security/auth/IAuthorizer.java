package backtype.storm.security.auth;

import java.util.Map;

/**
 * Nimbus could be configured with an authorization plugin.
 * If not specified, all requests are authorized.
 * 
 * You could specify the authorization plugin via storm parameter. For example:
 *  storm -c nimbus.authorization.class=backtype.storm.security.auth.NoopAuthorizer ...
 *  
 * You could also specify it via storm.yaml:
 *   nimbus.authorization.class: backtype.storm.security.auth.NoopAuthorizer
 */
public interface IAuthorizer {
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    void prepare(Map storm_conf);
    
    /**
     * permit() method is invoked for each incoming Thrift request.
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf);
}
