package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
    Set<Integer> allPorts;

    public SupervisorDetails(String id, Object meta){
        this.id = id;
        this.meta = meta;
        allPorts = new HashSet();
    }
    
    public SupervisorDetails(String id, Object meta, Collection<Number> allPorts){
        this.id = id;
        this.meta = meta;
        setAllPorts(allPorts);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<Number> allPorts){
        this.id = id;
        this.host = host;
        this.schedulerMeta = schedulerMeta;

        setAllPorts(allPorts);
    }

    private void setAllPorts(Collection<Number> allPorts) {
        this.allPorts = new HashSet<Integer>();
        if(allPorts!=null) {
            for(Number n: allPorts) {
                this.allPorts.add(n.intValue());
            }
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
    
    public Set<Integer> getAllPorts() {
        return allPorts;
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }
}
