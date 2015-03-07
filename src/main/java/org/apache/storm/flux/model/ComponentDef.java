package org.apache.storm.flux.model;

/**
 * Abstract parent class of component definitions
 * (spouts/bolts)
 */
public abstract class ComponentDef {
    private String id;
    private String className;
    private int parallelism;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
