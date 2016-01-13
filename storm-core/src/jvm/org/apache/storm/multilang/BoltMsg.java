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
package org.apache.storm.multilang;

import java.util.List;

/**
 * BoltMsg is an object that represents the data sent from a shell component to
 * a bolt process that implements a multi-language protocol. It is the union of
 * all data types that a bolt can receive from Storm.
 *
 * BoltMsgs are objects sent to the ISerializer interface, for serialization
 * according to the wire protocol implemented by the serializer. The BoltMsg
 * class allows for a decoupling between the serialized representation of the
 * data and the data itself.
 *
 */
public class BoltMsg {
    private String id;
    private String comp;
    private String stream;
    private long task;
    private List<Object> tuple;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getComp() {
        return comp;
    }

    public void setComp(String comp) {
        this.comp = comp;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public long getTask() {
        return task;
    }

    public void setTask(long task) {
        this.task = task;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public void setTuple(List<Object> tuple) {
        this.tuple = tuple;
    }
}
