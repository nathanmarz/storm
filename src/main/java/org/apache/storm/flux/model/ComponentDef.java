package org.apache.storm.flux.model;

/**
 * Abstract parent class of component definitions
 * (spouts/bolts)
 */
public abstract class ComponentDef {
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
