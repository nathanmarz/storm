/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.scheduler.ExecutorDetails;

public class Component {
    public enum ComponentType {
        SPOUT(1), BOLT(2);
        private int value;

        private ComponentType(int value) {
            this.value = value;
        }
    }

    public String id;
    public List<String> parents = null;
    public List<String> children = null;
    public List<ExecutorDetails> execs = null;
    public ComponentType type = null;

    public Component(String id) {
        this.parents = new ArrayList<String>();
        this.children = new ArrayList<String>();
        this.execs = new ArrayList<ExecutorDetails>();
        this.id = id;
    }

    @Override
    public String toString() {
        String retVal = "{id: " + this.id + " Parents: " + this.parents.toString() + " Children: " + this.children.toString() + " Execs: " + this.execs + "}";
        return retVal;
    }
}
