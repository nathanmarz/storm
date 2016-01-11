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
package org.apache.storm.hdfs.bolt.sync;


import org.apache.storm.tuple.Tuple;

/**
 * SyncPolicy implementation that will trigger a
 * file system sync after a certain number of tuples
 * have been processed.
 */
public class CountSyncPolicy implements SyncPolicy {
    private int count;
    private int executeCount = 0;

    public CountSyncPolicy(int count){
        this.count = count;
    }

    @Override
    public boolean mark(Tuple tuple, long offset) {
        this.executeCount++;
        return this.executeCount >= this.count;
    }

    @Override
    public void reset() {
        this.executeCount = 0;
    }
}
