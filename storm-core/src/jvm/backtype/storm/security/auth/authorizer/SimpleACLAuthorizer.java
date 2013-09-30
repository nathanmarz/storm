package backtype.storm.security.auth.authorizer;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IPrincipalToLocal;

/**
 * An authorization implementation that simply checks if a user is allowed to perform specific
 * operations.
 */
public class SimpleACLAuthorizer implements IAuthorizer {
    protected Set<String> _userCommands = new HashSet<String>(Arrays.asList("submitTopology", "fileUpload", "getNimbusConf", "getClusterInfo"));
    protected Set<String> _supervisorCommands = new HashSet<String>(Arrays.asList("fileDownload"));
    protected Set<String> _topoCommands = new HashSet<String>(Arrays.asList("killTopology","rebalance","activate","deactivate","getTopologyConf","getTopology","getUserTopology","getTopologyInfo"));

    protected Set<String> _admins;
    protected Set<String> _supervisors;
    protected IPrincipalToLocal _ptol;

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    @Override
    public void prepare(Map conf) {
        _admins = new HashSet<String>();
        _supervisors = new HashSet<String>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            _admins.addAll((Collection<String>)conf.get(Config.NIMBUS_ADMINS));
        }
        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            _supervisors.addAll((Collection<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }
        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_conf configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map topology_conf) {

        String principal = context.principal().getName();
        String user = _ptol.toLocal(context.principal());

        if (_admins.contains(principal) || _admins.contains(user)) {
            return true;
        }

        if (_supervisors.contains(principal) || _supervisors.contains(user)) {
            return _supervisorCommands.contains(operation);
        }

        if (_userCommands.contains(operation)) {
            return true;
        }

        if (_topoCommands.contains(operation)) {
            Set topoUsers = new HashSet<String>();
            if (topology_conf.containsKey(Config.TOPOLOGY_USERS)) {
                topoUsers.addAll((Collection<String>)topology_conf.get(Config.TOPOLOGY_USERS));
            }

            if (topoUsers.contains(principal) || topoUsers.contains(user)) {
                return true;
            }
        }
         
        return false;
    }
}
