package org.apache.storm.flux.model;

/**
 * A bean reference is simply a string pointer to another id.
 */
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
