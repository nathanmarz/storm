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

package org.apache.storm.utils;

import org.apache.storm.multilang.BoltMsg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A data structure for ShellBolt which includes two queues (FIFO),
 * which one is for task ids (unbounded), another one is for bolt msg (bounded).
 */
public class ShellBoltMessageQueue implements Serializable {
    private final LinkedList<List<Integer>> taskIdsQueue = new LinkedList<>();
    private final LinkedBlockingQueue<BoltMsg> boltMsgQueue;

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();

    public ShellBoltMessageQueue(int boltMsgCapacity) {
        if (boltMsgCapacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.boltMsgQueue = new LinkedBlockingQueue<>(boltMsgCapacity);
    }

    public ShellBoltMessageQueue() {
        this(Integer.MAX_VALUE);
    }

    /**
     * put list of task id to its queue
     * @param taskIds task ids that received the tuples
     */
    public void putTaskIds(List<Integer> taskIds) {
        taskIdsQueue.add(taskIds);
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * put bolt message to its queue
     * @param boltMsg BoltMsg to pass to subprocess
     * @throws InterruptedException
     */
    public void putBoltMsg(BoltMsg boltMsg) throws InterruptedException {
        boltMsgQueue.put(boltMsg);
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * poll() is a core feature of ShellBoltMessageQueue.
     * It retrieves and removes the head of one queues, waiting up to the
     * specified wait time if necessary for an element to become available.
     * There's priority that what queue it retrieves first, taskIds is higher than boltMsgQueue.
     *
     * @param timeout how long to wait before giving up, in units of unit
     * @param unit a TimeUnit determining how to interpret the timeout parameter
     * @return List\<Integer\> if task id is available,
     * BoltMsg if task id is not available but bolt message is available,
     * null if the specified waiting time elapses before an element is available.
     * @throws InterruptedException
     */
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        takeLock.lockInterruptibly();
        long nanos = unit.toNanos(timeout);
        try {
            // wait for available queue
            while (taskIdsQueue.peek() == null && boltMsgQueue.peek() == null) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }

            // taskIds first
            List<Integer> taskIds = taskIdsQueue.peek();
            if (taskIds != null) {
                taskIds = taskIdsQueue.poll();
                return taskIds;
            }

            // boltMsgQueue should have at least one entry at the moment
            return boltMsgQueue.poll();
        } finally {
            takeLock.unlock();
        }
    }

}
