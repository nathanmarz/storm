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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.query;

import backtype.storm.tuple.ITuple;

import java.io.Serializable;

/**
 * Default interface for mapping a {@link backtype.storm.tuple.ITuple} to a table name.
 *
 */
public interface CQLTableTupleMapper extends Serializable {

    /**
     * Returns a table's name from the specified tuple.
     *
     * @param tuple the incoming tuple.
     * @return the table name.
     */
    String map(ITuple tuple);

}
