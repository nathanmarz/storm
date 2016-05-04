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
package org.apache.storm.cassandra.bolt;

import org.apache.storm.tuple.Tuple;
import com.datastax.driver.core.BatchStatement;

import java.util.List;

/**
 * Simple class to pair a list of tuples with a single batch statement.
 */
public class PairBatchStatementTuples {

    private final List<Tuple> inputs;

    private final BatchStatement statement;

    /**
     * Creates a new {@link PairBatchStatementTuples} instance.
     * @param inputs List of inputs attached to this batch.
     * @param statement The batch statement.
     */
    public PairBatchStatementTuples(List<Tuple> inputs, BatchStatement statement) {
        this.inputs = inputs;
        this.statement = statement;
    }

    public List<Tuple> getInputs() {
        return inputs;
    }

    public BatchStatement getStatement() {
        return statement;
    }
}
