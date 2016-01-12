/*
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
package org.apache.storm.flux.model;

import org.apache.storm.Config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A representation of a Java object that given a className, constructor arguments,
 * and properties, can be instantiated.
 */
public class ObjectDef {
    private String className;
    private List<Object> constructorArgs;
    private boolean hasReferences;
    private List<PropertyDef> properties;
    private List<ConfigMethodDef> configMethods;

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
                } else {
                    newVal.add(obj);
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

    public List<ConfigMethodDef> getConfigMethods() {
        return configMethods;
    }

    public void setConfigMethods(List<ConfigMethodDef> configMethods) {
        this.configMethods = configMethods;
    }
}
