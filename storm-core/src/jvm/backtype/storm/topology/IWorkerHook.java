package backtype.storm.topology;

import backtype.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Defines the interface for hooks to be called during worker startup and shutdown.
 */
public interface IWorkerHook extends Serializable {

    /**
     * Tells the hook the worker is starting up.
     */
    // it'd be better for context to be a TopologyContext, but that's in the context of a single task, which
    // doesn't apply at this time, or for the whole worker
    void start(Map conf, WorkerTopologyContext context, List taskIds);

    /**
     * Tells the hook the worker is shutting down.
     */
    // TODO this is not actually called right now
    void shutdown();
}
  
