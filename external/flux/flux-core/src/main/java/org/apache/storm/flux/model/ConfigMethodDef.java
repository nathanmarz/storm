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
