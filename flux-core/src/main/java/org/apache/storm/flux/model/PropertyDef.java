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
