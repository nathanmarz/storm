package org.apache.storm.flux.model;

public class StreamDef {

    private String name;
    private String from;
    private String to;
    private GroupingDef grouping;

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public GroupingDef getGrouping() {
        return grouping;
    }

    public void setGrouping(GroupingDef grouping) {
        this.grouping = grouping;
    }
}
