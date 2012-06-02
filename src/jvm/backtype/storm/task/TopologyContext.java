package backtype.storm.task;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.state.ISubscribedState;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.NotImplementedException;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 *
 * <p>The TopologyContext is also used to declare ISubscribedState objects to
 * synchronize state with StateSpouts this object is subscribed to.</p>
 */
public class TopologyContext extends WorkerTopologyContext {
    private Integer _taskId;
    private Map<String, Object> _taskData = new HashMap<String, Object>();
    private List<ITaskHook> _hooks = new ArrayList<ITaskHook>();
    private Map<String, Object> _executorData;

    
    public TopologyContext(StormTopology topology, Map stormConf,
            Map<Integer, String> taskToComponent, Map<String, List<Integer>> componentToSortedTasks,
            Map<String, Map<String, Fields>> componentToStreamToFields,
            String stormId, String codeDir, String pidDir, Integer taskId,
            Integer workerPort, List<Integer> workerTasks, Map<String, Object> defaultResources,
            Map<String, Object> userResources, Map<String, Object> executorData) {
        super(topology, stormConf, taskToComponent, componentToSortedTasks,
                componentToStreamToFields, stormId, codeDir, pidDir,
                workerPort, workerTasks, defaultResources, userResources);
        _taskId = taskId;
        _executorData = executorData;
    }

    /**
     * All state from all subscribed state spouts streams will be synced with
     * the provided object.
     * 
     * <p>It is recommended that your ISubscribedState object is kept as an instance
     * variable of this object. The recommended usage of this method is as follows:</p>
     *
     * <p>
     * _myState = context.setAllSubscribedState(new MyState());
     * </p>
     * @param obj Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setAllSubscribedState(T obj) {
        //check that only subscribed to one component/stream for statespout
        //setsubscribedstate appropriately
        throw new NotImplementedException();
    }


    /**
     * Synchronizes the default stream from the specified state spout component
     * id with the provided ISubscribedState object.
     *
     * <p>The recommended usage of this method is as follows:</p>
     * <p>
     * _myState = context.setSubscribedState(componentId, new MyState());
     * </p>
     *
     * @param componentId the id of the StateSpout component to subscribe to
     * @param obj Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setSubscribedState(String componentId, T obj) {
        return setSubscribedState(componentId, Utils.DEFAULT_STREAM_ID, obj);
    }

    /**
     * Synchronizes the specified stream from the specified state spout component
     * id with the provided ISubscribedState object.
     *
     * <p>The recommended usage of this method is as follows:</p>
     * <p>
     * _myState = context.setSubscribedState(componentId, streamId, new MyState());
     * </p>
     *
     * @param componentId the id of the StateSpout component to subscribe to
     * @param streamId the stream to subscribe to
     * @param obj Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setSubscribedState(String componentId, String streamId, T obj) {
        throw new NotImplementedException();
    }

    /**
     * Gets the task id of this task.
     * 
     * @return the task id
     */
    public int getThisTaskId() {
        return _taskId;
    }

    /**
     * Gets the component id for this task. The component id maps
     * to a component id specified for a Spout or Bolt in the topology definition.
     * @return
     */
    public String getThisComponentId() {
        return getComponentId(_taskId);
    }

    /**
     * Gets the declared output fields for the specified stream id for the component
     * this task is a part of.
     */
    public Fields getThisOutputFields(String streamId) {
        return getComponentOutputFields(getThisComponentId(), streamId);
    }

    /**
     * Gets the set of streams declared for the component of this task.
     */
    public Set<String> getThisStreams() {
        return getComponentStreams(getThisComponentId());
    }

    /**
     * Gets the index of this task id in getComponentTasks(getThisComponentId()).
     * An example use case for this method is determining which task
     * accesses which resource in a distributed resource to ensure an even distribution.
     */
    public int getThisTaskIndex() {
        List<Integer> tasks = new ArrayList<Integer>(getComponentTasks(getThisComponentId()));
        Collections.sort(tasks);
        for(int i=0; i<tasks.size(); i++) {
            if(tasks.get(i) == getThisTaskId()) {
                return i;
            }
        }
        throw new RuntimeException("Fatal: could not find this task id in this component");
    }
    
    /**
     * Gets the declared inputs to this component.
     * 
     * @return A map from subscribed component/stream to the grouping subscribed with.
     */
    public Map<GlobalStreamId, Grouping> getThisSources() {
        return getSources(getThisComponentId());
    }

    /**
     * Gets information about who is consuming the outputs of this component, and how.
     *
     * @return Map from stream id to component id to the Grouping used.
     */
    public Map<String, Map<String, Grouping>> getThisTargets() {
        return getTargets(getThisComponentId());
    }
    
    public void setTaskData(String name, Object data) {
        _taskData.put(name, data);
    }
    
    public Object getTaskData(String name) {
        return _taskData.get(name);
    }

    public void setExecutorData(String name, Object data) {
        _executorData.put(name, data);
    }
    
    public Object getExecutorData(String name) {
        return _executorData.get(name);
    }    
    
    public void addTaskHook(ITaskHook hook) {
        hook.prepare(_stormConf, this);
        _hooks.add(hook);
    }
    
    public Collection<ITaskHook> getHooks() {
        return _hooks;
    }
}