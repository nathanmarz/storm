package org.apache.storm.flux.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BeanDef {
    private String id;
    private String className;
    private List<Object> constructorArgs;
    private boolean hasReferences;
    private List<PropertyDef> properties;

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

    public List<Object> getConstructorArgs() {
        return constructorArgs;
    }

    public void setConstructorArgs(List<Object> constructorArgs) {

        List<Object> newVal = new ArrayList<Object>();
        for(Object obj : constructorArgs){
            if(obj instanceof LinkedHashMap){
                Map map = (Map)obj;
                if(map.containsKey("ref") && map.size() == 1){
                    newVal.add(new BeanReference((String)map.get("ref")));
                    this.hasReferences = true;
                }
            } else {
                newVal.add(obj);
            }
        }
        this.constructorArgs = newVal;
    }

    public boolean hasConstructorArgs(){
        return this.constructorArgs != null && this.constructorArgs.size() > 0;
    }

    public boolean hasReferences(){
        return this.hasReferences;
    }

    public List<PropertyDef> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyDef> properties) {
        this.properties = properties;
    }
}
