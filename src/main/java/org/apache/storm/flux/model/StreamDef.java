package org.apache.storm.flux.model;

/**
 * Represents a stream of tuples from one Storm component (Spout or Bolt) to another (an edge in the topology DAG).
 *
 * Required fields are `from` and `to`, which define the source and destination, and the stream `grouping`.
 *
 */
public class StreamDef {

    private String name; // not used, placeholder for GUI, etc.
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
