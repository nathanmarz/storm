/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import java.io.PrintStream;
import java.util.TreeSet;

public class ProgressTracker {

  TreeSet<FileOffset> offsets = new TreeSet<>();

  public synchronized void recordAckedOffset(FileOffset newOffset) {
    if(newOffset==null) {
      return;
    }
    offsets.add(newOffset);

    FileOffset currHead = offsets.first();

    if( currHead.isNextOffset(newOffset) ) { // check is a minor optimization
      trimHead();
    }
  }

  // remove contiguous elements from the head of the heap
  // e.g.:  1,2,3,4,10,11,12,15  =>  4,10,11,12,15
  private synchronized void trimHead() {
    if(offsets.size()<=1) {
      return;
    }
    FileOffset head = offsets.first();
    FileOffset head2 = offsets.higher(head);
    if( head.isNextOffset(head2) ) {
      offsets.pollFirst();
      trimHead();
    }
    return;
  }

  public synchronized FileOffset getCommitPosition() {
    if(!offsets.isEmpty()) {
      return offsets.first().clone();
    }
    return null;
  }

  public synchronized void dumpState(PrintStream stream) {
    stream.println(offsets);
  }

  public synchronized int size() {
    return offsets.size();
  }
}
