package backtype.storm.task;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WorkerTopologyContext extends GeneralTopologyContext {
    private Integer _workerPort;
    private List<Integer> _workerTasks;
    private String _codeDir;
    private String _pidDir;
    
    public WorkerTopologyContext(StormTopology topology, Map stormConf,
            Map<Integer, String> taskToComponent, Map<String, List<Integer>> componentToSortedTasks,
            Map<String, Map<String, Fields>> componentToStreamToFields, String stormId,
            String codeDir, String pidDir, Integer workerPort,
            List<Integer> workerTasks) {
        super(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId);
        _codeDir = codeDir;
        try {
            if(pidDir!=null) {
                _pidDir = new File(pidDir).getCanonicalPath();
            } else {
                _pidDir = null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not get canonical path for " + _pidDir, e);
        }
        _workerPort = workerPort;
        _workerTasks = workerTasks;
    }

    /**
     * Gets all the task ids that are running in this worker process
     * (including the task for this task).
     */
    public List<Integer> getThisWorkerTasks() {
        return _workerTasks;
    }
    
    public Integer getThisWorkerPort() {
        return _workerPort;
    }

    /**
     * Gets the location of the external resources for this worker on the
     * local filesystem. These external resources typically include bolts implemented
     * in other languages, such as Ruby or Python.
     */
    public String getCodeDir() {
        return _codeDir;
    }

    /**
     * If this task spawns any subprocesses, those subprocesses must immediately
     * write their PID to this directory on the local filesystem to ensure that
     * Storm properly destroys that process when the worker is shutdown.
     */
    public String getPIDDir() {
        return _pidDir;
    }
}
