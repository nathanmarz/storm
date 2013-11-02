package backtype.storm.topology;

import backtype.storm.task.WorkerTopologyContext;

import java.util.List;
import java.util.Map;

/**
 * @author Dane Hammer (dh015921)
 */
// FIXME delete this before merging
public class TestWorkerHook implements IWorkerHook {

    public Map conf;
    public WorkerTopologyContext context;
    public boolean shutdown = false;

    @Override
    public void start(Map conf, WorkerTopologyContext context, List taskIds) {
        this.conf = conf;
        this.context = context;
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }
}
