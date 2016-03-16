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
package org.apache.storm.topology;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import java.util.List;


public class BasicOutputCollector implements IBasicOutputCollector {
    private OutputCollector out;
    private Tuple inputTuple;

    public BasicOutputCollector(OutputCollector out) {
        this.out = out;
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, inputTuple, tuple);
    }

    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public void setContext(Tuple inputTuple) {
        this.inputTuple = inputTuple;
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        out.emitDirect(taskId, streamId, inputTuple, tuple);
    }

    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }

    /**
    * Resets the message timeout for any tuple trees to which the given tuple belongs.
    * The timeout is reset to Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS.
    * Note that this is an expensive operation, and should be used sparingly.
    * @param tuple the tuple to reset timeout for
    */
    public void resetTimeout(Tuple tuple){
        out.resetTimeout(tuple);
    }

    protected IOutputCollector getOutputter() {
        return out;
    }

    public void reportError(Throwable t) {
        out.reportError(t);
    }
}
