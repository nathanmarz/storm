package org.apache.storm.flux.model;


public class BeanReference {
    public String id;

    public BeanReference(){}

    public BeanReference(String id){
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
