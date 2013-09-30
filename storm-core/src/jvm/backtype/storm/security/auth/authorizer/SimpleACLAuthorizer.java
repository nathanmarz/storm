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
    protected Set<String> userCommands = new HashSet<String>(Arrays.asList("submitTopology", "fileUpload", "getNimbusConf", "getClusterInfo"));
    protected Set<String> supervisorCommands = new HashSet<String>(Arrays.asList("fileDownload"));
    protected Set<String> topoCommands = new HashSet<String>(Arrays.asList("killTopology","rebalance","activate","deactivate","getTopologyConf","getTopology","getUserTopology","getTopologyInfo"));

    protected Set<String> admins;
    protected Set<String> supervisors;
    protected IPrincipalToLocal ptol;

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    @Override
    public void prepare(Map conf) {
        admins = new HashSet<String>();
        supervisors = new HashSet<String>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            admins.addAll((Collection<String>)conf.get(Config.NIMBUS_ADMINS));
        }
        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            supervisors.addAll((Collection<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }
        ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
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
        String user = ptol.toLocal(context.principal());

        if (admins.contains(principal) || admins.contains(user)) {
            return true;
        }

        if (supervisors.contains(principal) || supervisors.contains(user)) {
            return supervisorCommands.contains(operation);
        }

        if (userCommands.contains(operation)) {
            return true;
        }

        if (topoCommands.contains(operation)) {
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
