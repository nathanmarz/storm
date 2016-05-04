/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.spout;

import java.util.List;

import org.apache.storm.spout.ISpoutOutputCollector;

/**
 * Mock of ISpoutOutputCollector
 */
public class SpoutOutputCollectorMock implements ISpoutOutputCollector {
  //comma separated offsets
  StringBuilder emittedOffset;
  
  public SpoutOutputCollectorMock() {
    emittedOffset = new StringBuilder();
  }
  
  public String getOffsetSequenceAndReset() {
    String ret = null;
    if(emittedOffset.length() > 0) {
      emittedOffset.setLength(emittedOffset.length()-1);
      ret = emittedOffset.toString();
      emittedOffset.setLength(0);
    }
    return ret;
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    MessageId mid = (MessageId)messageId;
    String pid = mid.getPartitionId();
    String offset = mid.getOffset();
    emittedOffset.append(pid+"_"+offset+",");
    return null;
  }

  @Override
  public void emitDirect(int arg0, String arg1, List<Object> arg2, Object arg3) {
  }

  @Override
  public void reportError(Throwable arg0) {
  }

  @Override
  public long getPendingCount() {
    return 0;
  }
}
