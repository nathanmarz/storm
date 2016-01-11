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

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.util.List;

public interface HBaseValueMapper extends Serializable {
    /**
     *
     * @param input tuple.
     * @param result HBase lookup result instance.
     * @return list of values that should be emitted by the lookup bolt.
     * @throws Exception
     */
    public List<Values> toValues(ITuple input, Result result) throws Exception;

    /**
     * declares the output fields for the lookup bolt.
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
