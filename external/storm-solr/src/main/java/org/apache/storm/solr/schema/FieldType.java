/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.solr.schema;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

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
