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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.trident.windowing;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.spout.IBatchID;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This {@code ITridentWindowManager} instance stores all the tuples and trigger related information inmemory.
 */
public class InMemoryTridentWindowManager extends AbstractTridentWindowManager<TridentTuple> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryTridentWindowManager.class);

    public InMemoryTridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore, Aggregator aggregator,
                                        BatchOutputCollector delegateCollector) {
        super(windowConfig, windowTaskId, windowStore, aggregator, delegateCollector);
    }

    @Override
    protected void initialize() {
        LOG.debug("noop in initialize");
    }

    @Override
    public List<TridentTuple> getTridentTuples(List<TridentTuple> tridentBatchTuples) {
        return tridentBatchTuples;
    }

    @Override
    public void onTuplesExpired(List<TridentTuple> expiredTuples) {
        LOG.debug("InMemoryTridentWindowManager.onTuplesExpired");
    }

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples) {
        LOG.debug("Adding tuples to window-manager for batch: [{}]", batchId);
        for (TridentTuple tridentTuple : tuples) {
            windowManager.add(tridentTuple);
        }
    }

    public String getBatchTxnId(Object batchId) {
        if (!(batchId instanceof IBatchID)) {
            throw new IllegalArgumentException("argument should be an IBatchId instance");
        }
        return ((IBatchID) batchId).getId().toString();
    }

}