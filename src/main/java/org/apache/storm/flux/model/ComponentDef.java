package org.apache.storm.flux.model;

import java.util.List;

/**
 * Abstract parent class of component definitions
 * (spouts/bolts)
 */
public abstract class ComponentDef {
    private String id;
    private String className;
    private int parallelism;
    private List<Object> constructorArgs;

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

    public List<Object> getConstructorArgs() {
        return constructorArgs;
    }

    public void setConstructorArgs(List<Object> constructorArgs) {
        this.constructorArgs = constructorArgs;
    }

    public boolean hasConstructorArgs(){
        return this.constructorArgs != null && this.constructorArgs.size() > 0;
    }
}
