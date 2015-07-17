package org.apache.storm.solr.schema;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

/**
 * Created by hlouro on 7/27/15.
 */
public class FieldType implements Serializable {
    private String name;
    @SerializedName("class")
    private String clazz;
    private boolean multiValued;

    public String getName() {
        return name;
    }

    public String getClazz() {
        return clazz;
    }

    public boolean isMultiValued() {
        return multiValued;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public void setMultiValued(boolean multiValued) {
        this.multiValued = multiValued;
    }

    @Override
    public String toString() {
        return "FieldType{" +
                "name='" + name + '\'' +
                ", clazz='" + clazz + '\'' +
                ", multiValued=" + multiValued +
                '}';
    }
}
