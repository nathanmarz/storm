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
package org.apache.storm.hdfs.common;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

public interface Partitioner extends Serializable{

    /**
     * Return a relative path that the tuple should be written to. For example, if an HdfsBolt were configured to write
     * to /common/output and a partitioner returned "/foo" then the bolt should open a file in "/common/output/foo"
     *
     * A best practice is to use Path.SEPARATOR instead of a literal "/"
     *
     * @param tuple The tuple for which the relative path is being calculated.
     * @return
     */
    public String getPartitionPath(final Tuple tuple);
}
