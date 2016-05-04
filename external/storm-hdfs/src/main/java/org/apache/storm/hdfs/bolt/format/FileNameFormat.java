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
package org.apache.storm.hdfs.bolt.format;

import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Formatter interface for determining HDFS file names.
 *
 */
public interface FileNameFormat extends Serializable {

    void prepare(Map conf, TopologyContext topologyContext);

    /**
     * Returns the filename the HdfsBolt will create.
     * @param rotation the current file rotation number (incremented on every rotation)
     * @param timeStamp current time in milliseconds when the rotation occurs
     * @return
     */
    String getName(long rotation, long timeStamp);

    String getPath();
}
