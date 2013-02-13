package backtype.storm.security.auth;

/**
 * Nimbus could be configured with an authorization plugin.
 * If not specified, all requests are authorized.
 * 
 * You could specify the authorization plugin via storm parameter. For example:
 *  storm -c nimbus.authorization.classname=backtype.storm.security.auth.DefaultAuthorizer ...
 *  
 * You could also specify it via storm.yaml:
 *   nimbus.authorization.classname: backtype.storm.security.auth.DefaultAuthorizer
 */
public interface IAuthorization {
	/**
	 * permit() method is invoked for each incoming Thrift request.
	 * @param contrext request context includes info about 
	 *      		   (1) remote address/subject, 
	 *                 (2) operation
	 *                 (3) configuration of targeted topology 
	 * @return true if the request is authorized, false if reject
	 */
	public boolean permit(ReqContext context);
}
