package backtype.storm.testing;

import backtype.storm.ILocalCluster;

/**
 * This is the core interface for the storm java testing, usually
 * we put our java unit testing logic in the run method. A sample
 * code will be:
 * <code>
 * Testing.withSimulatedTimeLocalCluster(new TestJob() {
 *     public void run(Cluster cluster) {
 *         // your testing logic here.
 *     }
 * });
 */
public interface TestJob {
	/**
	 * run the testing logic with the cluster.
	 * 
	 * @param cluster the cluster which created by <code>Testing.withSimulatedTimeLocalCluster</code>
	 *        and <code>Testing.withTrackedCluster</code>.
	 */
    public void run(ILocalCluster cluster) throws Exception;
}
