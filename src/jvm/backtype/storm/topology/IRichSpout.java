package backtype.storm.topology;

import backtype.storm.spout.ISpout;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 *
 */
public interface IRichSpout extends ISpout, IComponent {
    /**
     * Returns if this spout is allowed to execute as multiple tasks. If
     * this method returns false, the planner will ignore the parallelism
     * assigned to this component and only use one task to execute this spout.
     *
     * @return Whether this spout is allowed to execute as multiple tasks
     */
    public boolean isDistributed();
}
