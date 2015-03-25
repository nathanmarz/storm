package org.apache.storm.flux.model;

public class TopologySourceDef extends ObjectDef {
    public static final String DEFAULT_METHOD_NAME = "getTopology";

    private String methodName;

    public TopologySourceDef(){
        this.methodName = DEFAULT_METHOD_NAME;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }
}
