package org.apache.storm.flux.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigMethodDef {
    private String name;
    private List<Object> args;
    private boolean hasReferences = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Object> getArgs() {
        return args;
    }

    public void setArgs(List<Object> args) {

        List<Object> newVal = new ArrayList<Object>();
        for(Object obj : args){
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
        this.args = newVal;
    }

    public boolean hasReferences(){
        return this.hasReferences;
    }
}
