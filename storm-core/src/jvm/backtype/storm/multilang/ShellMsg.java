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
package backtype.storm.multilang;

import java.util.ArrayList;
import java.util.List;

/**
 * ShellMsg is an object that represents the data sent to a shell component from
 * a process that implements a multi-language protocol. It is the union of all
 * data types that a component can send to Storm.
 *
 * <p>
 * ShellMsgs are objects received from the ISerializer interface, after the
 * serializer has deserialized the data from the underlying wire protocol. The
 * ShellMsg class allows for a decoupling between the serialized representation
 * of the data and the data itself.
 * </p>
 */
public class ShellMsg {
    private String command;
    private Object id;
    private List<String> anchors;
    private String stream;
    private long task;
    private String msg;
    private List<Object> tuple;
    private boolean needTaskIds;

    //metrics rpc 
    private String metricName;
    private Object metricParams;

    //logLevel
    public enum ShellLogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR;

        public static ShellLogLevel fromInt(int i) {
            switch (i) {
                case 0: return TRACE;
                case 1: return DEBUG;
                case 2: return INFO;
                case 3: return WARN;
                case 4: return ERROR;
                default: return INFO;
            }
        }
    }

    private ShellLogLevel logLevel = ShellLogLevel.INFO;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public List<String> getAnchors() {
        return anchors;
    }

    public void setAnchors(List<String> anchors) {
        this.anchors = anchors;
    }

    public void addAnchor(String anchor) {
        if (anchors == null) {
            anchors = new ArrayList<String>();
        }
        this.anchors.add(anchor);
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

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public void setTuple(List<Object> tuple) {
        this.tuple = tuple;
    }

    public void addTuple(Object tuple) {
        if (this.tuple == null) {
            this.tuple = new ArrayList<Object>();
        }
        this.tuple.add(tuple);
    }

    public boolean areTaskIdsNeeded() {
        return needTaskIds;
    }

    public void setNeedTaskIds(boolean needTaskIds) {
        this.needTaskIds = needTaskIds;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setMetricParams(Object metricParams) {
        this.metricParams = metricParams;
    }

    public Object getMetricParams() {
        return metricParams;
    }

    public ShellLogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = ShellLogLevel.fromInt(logLevel);
    }
}
