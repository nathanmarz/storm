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
package org.apache.storm.hdfs.trident.sync;

import org.apache.storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Interface for controlling when the HdfsBolt
 * syncs and flushes the filesystem.
 *
 */
public interface SyncPolicy extends Serializable {
    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple The tuple executed.
     * @param offset current offset for the file being written
     * @return true if a sync should be performed
     */
    boolean mark(TridentTuple tuple, long offset);


    /**
     * Called after the HdfsBolt performs a sync.
     *
     */
    void reset();

}
