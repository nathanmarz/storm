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
package org.apache.storm.flux.wrappers.bolts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * A generic `ShellBolt` implementation that allows you specify output fields
 * without having to subclass `ShellBolt` to do so.
 *
 */
public class FluxShellBolt extends ShellBolt implements IRichBolt{
    private String[] outputFields;
    private Map<String, Object> componentConfig;

    /**
     * Create a ShellBolt with command line arguments and output fields
     * @param command Command line arguments for the bolt
     * @param outputFields Names of fields the bolt will emit (if any).
     */

    public FluxShellBolt(String[] command, String[] outputFields){
        super(command);
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputFields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return this.componentConfig;
    }
}
