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
package org.apache.storm.hbase.bolt.mapper;


import org.apache.storm.tuple.Tuple;
import org.apache.storm.hbase.common.ColumnList;

import java.io.Serializable;

/**
 * Maps a <code>org.apache.storm.tuple.Tuple</code> object
 * to a row in an HBase table.
 */
public interface HBaseMapper extends Serializable {

    /**
     * Given a tuple, return the HBase rowkey.
     *
     * @param tuple
     * @return
     */
    byte[] rowKey(Tuple tuple);

    /**
     * Given a tuple, return a list of HBase columns to insert.
     *
     * @param tuple
     * @return
     */
    ColumnList columns(Tuple tuple);

}
