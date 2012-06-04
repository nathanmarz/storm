package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;

public class SupervisorDetails {

    String id;
    /**
     * hostname of this supervisor
     */
    String host;
    Object meta;
    /**
     * meta data configured for this supervisor
     */
    Object schedulerMeta;
    /**
     * all the ports of the supervisor
     */
    Collection<Integer> allPorts;

    public SupervisorDetails(String id, Object meta){
        this.id = id;
        this.meta = meta;
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<Integer> allPorts){
        this.id = id;
        this.host = host;
        this.schedulerMeta = schedulerMeta;

        this.allPorts = new ArrayList<Integer>();
        if (allPorts != null && !allPorts.isEmpty()) {
            this.allPorts.addAll(allPorts);
        }
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Object getMeta() {
        return meta;
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }
}
