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

/**
 * Represents a stream of tuples from one Storm component (Spout or Bolt) to another (an edge in the topology DAG).
 *
 * Required fields are `from` and `to`, which define the source and destination, and the stream `grouping`.
 *
 */
public class StreamDef {

    private String name; // not used, placeholder for GUI, etc.
    private String from;
    private String to;
    private GroupingDef grouping;

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public GroupingDef getGrouping() {
        return grouping;
    }

    public void setGrouping(GroupingDef grouping) {
        this.grouping = grouping;
    }
}
