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
package org.apache.storm;
import org.apache.storm.daemon.Shutdownable;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In local mode, {@code ProcessSimulator} keeps track of Shutdownable objects
 * in place of actual processes (in cluster mode).
 */
public class ProcessSimulator {
    private static Logger LOG = LoggerFactory.getLogger(ProcessSimulator.class);
    private static Object lock = new Object();
    protected static ConcurrentHashMap<String, Shutdownable> processMap = new ConcurrentHashMap<String, Shutdownable>();

    /**
     * Register a process' handle
     * 
     * @param pid
     * @param shutdownable
     */
    public static void registerProcess(String pid, Shutdownable shutdownable) {
        processMap.put(pid, shutdownable);
    }

    /**
     * Get all process handles
     * 
     * @return
     */
    public static Collection<Shutdownable> getAllProcessHandles() {
        return processMap.values();
    }

    /**
     * Kill a process
     * 
     * @param pid
     */
    public static void killProcess(String pid) {
        synchronized (lock) {
            LOG.info("Begin killing process " + pid);
            Shutdownable shutdownHandle = processMap.get(pid);
            if (shutdownHandle != null) {
                shutdownHandle.shutdown();
            }
            processMap.remove(pid);
            LOG.info("Successfully killed process " + pid);
        }
    }

    /**
     * Kill all processes
     */
    public static void killAllProcesses() {
        Set<String> pids = processMap.keySet();
        for (String pid : pids) {
            killProcess(pid);
        }
    }
}
