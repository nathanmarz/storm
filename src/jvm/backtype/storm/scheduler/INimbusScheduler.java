package backtype.storm.scheduler;

import backtype.storm.task.GeneralTopologyContext;
import java.util.List;
import java.util.Map;

public interface INimbusScheduler {
    void prepare(Map stormConf);
    List<WorkerSlot> availableSlots(Map<String, Object> existingSupervisorMeta, String topologyId, Map topologyConf, GeneralTopologyContext topology);
    void assignSlot(WorkerSlot slot);
}
