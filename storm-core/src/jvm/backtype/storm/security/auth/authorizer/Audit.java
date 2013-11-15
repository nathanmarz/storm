package backtype.storm.security.auth.authorizer;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.security.auth.ReqContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Audit {
    private static final Logger LOG = LoggerFactory.getLogger(Audit.class);

    /**
     * log each incoming Thrift request
     *
     * @param context       request context includes info about
     * @param operation     operation name
     * @param topology_conf configuration of targeted topology
     */
    public static void log(ReqContext context, String operation, Map topology_conf) {
        LOG.info("[req " + context.requestID() + "] Access "
                + " from:" + (context.remoteAddress() == null ? "null" : context.remoteAddress().toString())
                + (context.principal() == null ? "" : (" principal:" + context.principal()))
                + " op:" + operation
                + (topology_conf == null ? "" : (" topoology:" + topology_conf.get(Config.TOPOLOGY_NAME))));
    }
}
