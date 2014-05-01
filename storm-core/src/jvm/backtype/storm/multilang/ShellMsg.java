package backtype.storm.multilang;

import java.util.ArrayList;
import java.util.List;

/**
 * ShellMsg is an object that represents the data sent to a shell component from
 * a process that implements a multi-language protocol. It is the union of all
 * data types that a component can send to Storm.
 *
 * <p>
 * ShellMsgs are objects received from the ISerializer interface, after the
 * serializer has deserialized the data from the underlying wire protocol. The
 * ShellMsg class allows for a decoupling between the serialized representation
 * of the data and the data itself.
 * </p>
 */
public class ShellMsg {
    private String command;
    private Object id;
    private List<String> anchors;
    private String stream;
    private long task;
    private String msg;
    private List<Object> tuple;
    private boolean needTaskIds;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public List<String> getAnchors() {
        return anchors;
    }

    public void setAnchors(List<String> anchors) {
        this.anchors = anchors;
    }

    public void addAnchor(String anchor) {
        if (anchors == null) {
            anchors = new ArrayList<String>();
        }
        this.anchors.add(anchor);
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public long getTask() {
        return task;
    }

    public void setTask(long task) {
        this.task = task;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public void setTuple(List<Object> tuple) {
        this.tuple = tuple;
    }

    public void addTuple(Object tuple) {
        if (this.tuple == null) {
            this.tuple = new ArrayList<Object>();
        }
        this.tuple.add(tuple);
    }

    public boolean areTaskIdsNeeded() {
        return needTaskIds;
    }

    public void setNeedTaskIds(boolean needTaskIds) {
        this.needTaskIds = needTaskIds;
    }
}
