package org.apache.storm.solr.schema;

import java.io.Serializable;

/**
 * Created by hlouro on 7/27/15.
 */
public class Field implements Serializable {
    private String name;
    private String type;
    private boolean multiValued;

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isMultiValued() {
        return multiValued;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMultiValued(boolean multiValued) {
        this.multiValued = multiValued;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", multiValued=" + multiValued +
                '}';
    }
}
