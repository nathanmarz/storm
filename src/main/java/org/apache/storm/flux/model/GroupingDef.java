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

import java.util.List;

/**
 * Bean representation of a Storm stream grouping.
 */
public class GroupingDef {

    /**
     * Types of stream groupings Storm allows
     */
    public static enum Type {
        ALL,
        CUSTOM,
        DIRECT,
        SHUFFLE,
        LOCAL_OR_SHUFFLE,
        FIELDS,
        GLOBAL,
        NONE
    }

    private Type type;
    private String streamId;
    private List<String> args;
    private ObjectDef customClass;

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public ObjectDef getCustomClass() {
        return customClass;
    }

    public void setCustomClass(ObjectDef customClass) {
        this.customClass = customClass;
    }
}
