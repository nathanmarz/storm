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

import backtype.storm.task.OutputCollector;
import backtype.storm.utils.Utils;
import java.util.List;

/**
 * This output collector exposes the API for emitting tuples from an {@link backtype.storm.topology.IRichSpout}.
 * The main difference between this output collector and {@link OutputCollector}
 * for {@link backtype.storm.topology.IRichBolt} is that spouts can tag messages with ids so that they can be
 * acked or failed later on. This is the Spout portion of Storm's API to
 * guarantee that each message is fully processed at least once.
 */
public class SpoutOutputCollector implements ISpoutOutputCollector {
    ISpoutOutputCollector _delegate;

    public SpoutOutputCollector(ISpoutOutputCollector delegate) {
        _delegate = delegate;
    }

    /**
     * Emits a new tuple to the specified output stream with the given message ID.
     * When Storm detects that this tuple has been fully processed, or has failed
     * to be fully processed, the spout will receive an ack or fail callback respectively
     * with the messageId as long as the messageId was not null. If the messageId was null,
     * Storm will not track the tuple and no callback will be received. The emitted values must be 
     * immutable.
     *
     * @return the list of task ids that this tuple was sent to
     */
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return _delegate.emit(streamId, tuple, messageId);
    }

    /**
     * Emits a new tuple to the default output stream with the given message ID.
     * When Storm detects that this tuple has been fully processed, or has failed
     * to be fully processed, the spout will receive an ack or fail callback respectively
     * with the messageId as long as the messageId was not null. If the messageId was null,
     * Storm will not track the tuple and no callback will be received. The emitted values must be 
     * immutable.
     *
     * @return the list of task ids that this tuple was sent to
     */
    public List<Integer> emit(List<Object> tuple, Object messageId) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    /**
     * Emits a tuple to the default output stream with a null message id. Storm will
     * not track this message so ack and fail will never be called for this tuple. The
     * emitted values must be immutable.
     */
    public List<Integer> emit(List<Object> tuple) {
        return emit(tuple, null);
    }

    /**
     * Emits a tuple to the specified output stream with a null message id. Storm will
     * not track this message so ack and fail will never be called for this tuple. The
     * emitted values must be immutable.
     */
    public List<Integer> emit(String streamId, List<Object> tuple) {
        return emit(streamId, tuple, null);
    }

    /**
     * Emits a tuple to the specified task on the specified output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message. The emitted values must be 
     * immutable.
     */
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        _delegate.emitDirect(taskId, streamId, tuple, messageId);
    }

    /**
     * Emits a tuple to the specified task on the default output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message. The emitted values must be 
     * immutable.
     */
    public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }
    
    /**
     * Emits a tuple to the specified task on the specified output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message. The emitted values must be 
     * immutable.
     *
     * <p> Because no message id is specified, Storm will not track this message
     * so ack and fail will never be called for this tuple.</p>
     */
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        emitDirect(taskId, streamId, tuple, null);
    }

    /**
     * Emits a tuple to the specified task on the default output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message. The emitted values must be 
     * immutable.
     *
     * <p> Because no message id is specified, Storm will not track this message
     * so ack and fail will never be called for this tuple.</p>
     */
    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, tuple, null);
    }

    @Override
    public void reportError(Throwable error) {
        _delegate.reportError(error);
    }
}
