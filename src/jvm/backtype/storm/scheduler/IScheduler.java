package backtype.storm.scheduler;


public interface IScheduler {
    /**
     * Set assignments for the topologies which needs scheduling. The new assignments is available 
     * through <code>cluster.getAssignments()</code>
     *
     *@param topologies all the topologies in the cluster, some of them need schedule. Topologies object here 
     *       only contain static information about topologies. Information like assignments, slots are all in
     *       the <code>cluster</code>object.
     *@param cluster the cluster these topologies are running in. <code>cluster</code> contains everything user
     *       need to develop a new scheduling logic. e.g. supervisors information, available slots, current 
     *       assignments for all the topologies etc. User can set the new assignment for topologies using
     *       <code>cluster.setAssignmentById</code>
     */
    public void schedule(Topologies topologies, Cluster cluster);
}
