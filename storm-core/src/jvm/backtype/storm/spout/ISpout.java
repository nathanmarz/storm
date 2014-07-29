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
package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import java.util.Map;
import java.io.Serializable;

/**
 * ISpout is the core interface for implementing spouts. A Spout is responsible
 * for feeding messages into the topology for processing. For every tuple emitted by
 * a spout, Storm will track the (potentially very large) DAG of tuples generated
 * based on a tuple emitted by the spout. When Storm detects that every tuple in
 * that DAG has been successfully processed, it will send an ack message to the Spout.
 *
 * <p>If a tuple fails to be fully processed within the configured timeout for the
 * topology (see {@link backtype.storm.Config}), Storm will send a fail message to the spout
 * for the message.</p>
 *
 * <p> When a Spout emits a tuple, it can tag the tuple with a message id. The message id
 * can be any type. When Storm acks or fails a message, it will pass back to the
 * spout the same message id to identify which tuple it's referring to. If the spout leaves out
 * the message id, or sets it to null, then Storm will not track the message and the spout
 * will not receive any ack or fail callbacks for the message.</p>
 *
 * <p>Storm executes ack, fail, and nextTuple all on the same thread. This means that an implementor
 * of an ISpout does not need to worry about concurrency issues between those methods. However, it 
 * also means that an implementor must ensure that nextTuple is non-blocking: otherwise 
 * the method could block acks and fails that are pending to be processed.</p>
 */
public interface ISpout extends Serializable {
    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     *
     * <p>This includes the:</p>
     *
     * @param conf The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
     */
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector);

    /**
     * Called when an ISpout is going to be shutdown. There is no guarentee that close
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * <p>The one context where close is guaranteed to be called is a topology is
     * killed when running Storm in local mode.</p>
     */
    void close();
    
    /**
     * Called when a spout has been activated out of a deactivated mode.
     * nextTuple will be called on this spout soon. A spout can become activated
     * after having been deactivated when the topology is manipulated using the 
     * `storm` client. 
     */
    void activate();
    
    /**
     * Called when a spout has been deactivated. nextTuple will not be called while
     * a spout is deactivated. The spout may or may not be reactivated in the future.
     */
    void deactivate();

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the 
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    void nextTuple();

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier
     * has been fully processed. Typically, an implementation of this method will take that
     * message off the queue and prevent it from being replayed.
     */
    void ack(Object msgId);

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to be
     * fully processed. Typically, an implementation of this method will put that
     * message back on the queue to be replayed at a later time.
     */
    void fail(Object msgId);
}