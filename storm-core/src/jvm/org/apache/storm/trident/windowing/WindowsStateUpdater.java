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

import com.google.common.collect.Lists;
import org.apache.commons.lang.IllegalClassException;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * {@code StateUpdater<WindowState>} instance which removes successfully emitted triggers from store
 */
public class WindowsStateUpdater implements StateUpdater<WindowsState> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowsStateUpdater.class);

    private final WindowsStoreFactory windowStoreFactory;
    private WindowsStore windowsStore;

    public WindowsStateUpdater(WindowsStoreFactory windowStoreFactory) {
        this.windowStoreFactory = windowStoreFactory;
    }

    @Override
    public void updateState(WindowsState state, List<TridentTuple> tuples, TridentCollector collector) {
        Long currentTxId = state.getCurrentTxId();
        LOG.debug("Removing triggers using WindowStateUpdater, txnId: [{}] ", currentTxId);
        for (TridentTuple tuple : tuples) {
            try {
                Object fieldValue = tuple.getValueByField(WindowTridentProcessor.TRIGGER_FIELD_NAME);
                if(! (fieldValue instanceof WindowTridentProcessor.TriggerInfo)) {
                    throw new IllegalClassException(WindowTridentProcessor.TriggerInfo.class, fieldValue.getClass());
                }
                WindowTridentProcessor.TriggerInfo triggerInfo = (WindowTridentProcessor.TriggerInfo) fieldValue;
                String triggerCompletedKey = WindowTridentProcessor.getWindowTriggerInprocessIdPrefix(triggerInfo.windowTaskId)+currentTxId;

                LOG.debug("Removing trigger key [{}] and trigger completed key [{}] from store: [{}]", triggerInfo, triggerCompletedKey, windowsStore);

                windowsStore.removeAll(Lists.newArrayList(triggerInfo.generateTriggerKey(), triggerCompletedKey));
            } catch (Exception ex) {
                LOG.warn(ex.getMessage());
                collector.reportError(ex);
                throw new FailedException(ex);
            }
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        windowsStore = windowStoreFactory.create(conf);
    }

    @Override
    public void cleanup() {
        windowsStore.shutdown();
    }
}
