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
package org.apache.storm.coordination;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import java.util.List;

public class BatchOutputCollectorImpl extends BatchOutputCollector {
    OutputCollector _collector;
    
    public BatchOutputCollectorImpl(OutputCollector collector) {
        _collector = collector;
    }
    
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
        return _collector.emit(streamId, tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        _collector.emitDirect(taskId, streamId, tuple);
    }

    @Override
    public void reportError(Throwable error) {
        _collector.reportError(error);
    }
    
    public void ack(Tuple tup) {
        _collector.ack(tup);
    }
    
    public void fail(Tuple tup) {
        _collector.fail(tup);
    }
}
