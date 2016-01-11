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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

/**
 * A bolt abstraction for supporting time and count based sliding & tumbling windows.
 */
public interface IWindowedBolt extends IComponent {
    /**
     * This is similar to the {@link org.apache.storm.task.IBolt#prepare(Map, TopologyContext, OutputCollector)} except
     * that while emitting, the tuples are automatically anchored to the tuples in the inputWindow.
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    /**
     * Process the tuple window and optionally emit new tuples based on the tuples in the input window.
     */
    void execute(TupleWindow inputWindow);
    void cleanup();
}
