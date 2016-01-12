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
package org.apache.storm.hdfs.bolt.rotation;


import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * Used by the HdfsBolt to decide when to rotate files.
 *
 * The HdfsBolt will call the <code>mark()</code> method for every
 * tuple received. If the <code>mark()</code> method returns
 * <code>true</code> the HdfsBolt will perform a file rotation.
 *
 * After file rotation, the HdfsBolt will call the <code>reset()</code>
 * method.
 */
public interface FileRotationPolicy extends Serializable {
    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    boolean mark(Tuple tuple, long offset);


    /**
     * Called after the HdfsBolt rotates a file.
     *
     */
    void reset();
}
