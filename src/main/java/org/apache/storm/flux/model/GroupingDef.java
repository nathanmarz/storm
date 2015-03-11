package org.apache.storm.flux.model;


import java.util.List;

/**
 * Bean representation of a Storm stream grouoing.
 */
public class GroupingDef {

    /**
     * Types of stream groupings Storm allows.
     */
    public static enum Type {
        ALL,
        CUSTOM,
        DIRECT,
        SHUFFLE,
        LOCAL_OR_SHUFFLE,
        FIELDS,
        GLOBAL,
        NONE
    }

    private Type type;
    private String streamId;
    private List<String> args;
    private String customClass;

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getCustomClass() {
        return customClass;
    }

    public void setCustomClass(String customClass) {
        this.customClass = customClass;
    }
}
