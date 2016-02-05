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
package org.apache.storm.trident.state.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MicroBatchIBackingMap<T> implements IBackingMap<T> {
    IBackingMap<T> _delegate;
    Options _options;


    public static class Options implements Serializable {
        public int maxMultiGetBatchSize = 0; // 0 means delegate batch size = trident batch size.
        public int maxMultiPutBatchSize = 0;
    }

    public MicroBatchIBackingMap(final Options options, final IBackingMap<T> delegate) {
        _options = options;
        _delegate = delegate;
        assert options.maxMultiPutBatchSize >= 0;
        assert options.maxMultiGetBatchSize >= 0;
    }

    @Override
    public void multiPut(final List<List<Object>> keys, final List<T> values) {
        int thisBatchSize;
        if(_options.maxMultiPutBatchSize == 0) { thisBatchSize = keys.size(); }
        else { thisBatchSize = _options.maxMultiPutBatchSize; }

        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);
        LinkedList<T> valuesTodo = new LinkedList<T>(values);

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(thisBatchSize);
            List<T> valuesBatch = new ArrayList<T>(thisBatchSize);
            for(int i=0; i<thisBatchSize && !keysTodo.isEmpty(); i++) {
                keysBatch.add(keysTodo.removeFirst());
                valuesBatch.add(valuesTodo.removeFirst());
            }

            _delegate.multiPut(keysBatch, valuesBatch);
        }
    }

    @Override
    public List<T> multiGet(final List<List<Object>> keys) {
        int thisBatchSize;
        if(_options.maxMultiGetBatchSize == 0) { thisBatchSize = keys.size(); }
        else { thisBatchSize = _options.maxMultiGetBatchSize; }

        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);

        List<T> ret = new ArrayList<T>(keys.size());

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(thisBatchSize);
            for(int i=0; i<thisBatchSize && !keysTodo.isEmpty(); i++) {
                keysBatch.add(keysTodo.removeFirst());
            }

            List<T> retSubset = _delegate.multiGet(keysBatch);
            ret.addAll(retSubset);
        }

        return ret;
    }
}
