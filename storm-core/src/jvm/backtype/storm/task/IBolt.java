package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.io.Serializable;

/**
 * An IBolt represents a component that takes tuples as input and produces tuples
 * as output. An IBolt can do everything from filtering to joining to functions
 * to aggregations. It does not have to process a tuple immediately and may
 * hold onto tuples to process later.
 *
 * <p>A bolt's lifecycle is as follows:</p>
 *
 * <p>IBolt object created on client machine. The IBolt is serialized into the topology
 * (using Java serialization) and submitted to the master machine of the cluster (Nimbus).
 * Nimbus then launches workers which deserialize the object, call prepare on it, and then
 * start processing tuples.</p>
 *
 * <p>If you want to parameterize an IBolt, you should set the parameter's through its
 * constructor and save the parameterization state as instance variables (which will
 * then get serialized and shipped to every task executing this bolt across the cluster).</p>
 *
 * <p>When defining bolts in Java, you should use the IRichBolt interface which adds
 * necessary methods for using the Java TopologyBuilder API.</p>
 */
public interface IBolt extends Serializable {
    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     *
     * <p>This includes the:</p>
     * 
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. The IBolt does not have to process the Tuple
     * immediately. It is perfectly fine to hang onto a tuple and process it later
     * (for instance, to do an aggregation or join).
     *
     * <p>Tuples should be emitted using the OutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some point using the OutputCollector.
     * Otherwise, Storm will be unable to determine when tuples coming off the spouts
     * have been completed.</p>
     *
     * <p>For the common case of acking an input tuple at the end of the execute method,
     * see IBasicBolt which automates this.</p>
     * 
     * @param input The input tuple to be processed.
     */
    void execute(Tuple input);

    /**
     * Called when an IBolt is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * <p>The one context where cleanup is guaranteed to be called is when a topology
     * is killed when running Storm in local mode.</p>
     */
    void cleanup();
}