package org.apache.storm.flux.model;

public class PropertyDef {
    private String name;
    private Object value;
    private String ref;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        if(this.ref != null){
            throw new IllegalStateException("A property can only have a value OR a reference, not both.");
        }
        this.value = value;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        if(this.value != null){
            throw new IllegalStateException("A property can only have a value OR a reference, not both.");
        }
        this.ref = ref;
    }

    public boolean isReference(){
        return this.ref != null;
    }
}
