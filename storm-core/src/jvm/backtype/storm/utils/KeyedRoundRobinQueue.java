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
package backtype.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;

public class KeyedRoundRobinQueue<V> {
    private final Object _lock = new Object();
    private Semaphore _size = new Semaphore(0);
    private Map<Object, Queue<V>> _queues = new HashMap<Object, Queue<V>>();
    private List<Object> _keyOrder = new ArrayList<Object>();
    private int _currIndex = 0;

    public void add(Object key, V val) {
        synchronized(_lock) {
            Queue<V> queue = _queues.get(key);
            if(queue==null) {
                queue = new LinkedList<V>();
                _queues.put(key, queue);
                _keyOrder.add(key);
            }
            queue.add(val);
        }
        _size.release();
    }

    public V take() throws InterruptedException {
        _size.acquire();
        synchronized(_lock) {
            Object key = _keyOrder.get(_currIndex);
            Queue<V> queue = _queues.get(key);
            V ret = queue.remove();
            if(queue.isEmpty()) {
                _keyOrder.remove(_currIndex);
                _queues.remove(key);
                if(_keyOrder.size()==0) {
                    _currIndex = 0;
                } else {
                    _currIndex = _currIndex % _keyOrder.size();
                }
            } else {
                _currIndex = (_currIndex + 1) % _keyOrder.size();
            }
            return ret;
        }
    }
}
