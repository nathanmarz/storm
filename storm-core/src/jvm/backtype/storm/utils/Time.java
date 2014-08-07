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

import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Time {
    public static Logger LOG = LoggerFactory.getLogger(Time.class);    
    
    private static AtomicBoolean simulating = new AtomicBoolean(false);
    //TODO: should probably use weak references here or something
    private static volatile Map<Thread, AtomicLong> threadSleepTimes;
    private static final Object sleepTimesLock = new Object();
    
    private static AtomicLong simulatedCurrTimeMs; //should this be a thread local that's allowed to keep advancing?
    
    public static void startSimulating() {
        synchronized(sleepTimesLock) {
            simulating.set(true);
            simulatedCurrTimeMs = new AtomicLong(0);
            threadSleepTimes = new ConcurrentHashMap<Thread, AtomicLong>();
        }
    }
    
    public static void stopSimulating() {
        synchronized(sleepTimesLock) {
            simulating.set(false);             
            threadSleepTimes = null;  
        }
    }
    
    public static boolean isSimulating() {
        return simulating.get();
    }
    
    public static void sleepUntil(long targetTimeMs) throws InterruptedException {
        if(simulating.get()) {
            try {
                synchronized(sleepTimesLock) {
                    threadSleepTimes.put(Thread.currentThread(), new AtomicLong(targetTimeMs));
                }
                while(simulatedCurrTimeMs.get() < targetTimeMs) {
                    Thread.sleep(10);
                }
            } finally {
                synchronized(sleepTimesLock) {
                    if (simulating.get()) {
                        threadSleepTimes.remove(Thread.currentThread());
                    }
                }
            }
        } else {
            long sleepTime = targetTimeMs-currentTimeMillis();
            if(sleepTime>0) 
                Thread.sleep(sleepTime);
        }
    }
    
    public static void sleep(long ms) throws InterruptedException {
        sleepUntil(currentTimeMillis()+ms);
    }
    
    public static long currentTimeMillis() {
        if(simulating.get()) {
            return simulatedCurrTimeMs.get();
        } else {
            return System.currentTimeMillis();
        }
    }
    
    public static int currentTimeSecs() {
        return (int) (currentTimeMillis() / 1000);
    }
    
    public static void advanceTime(long ms) {
        if(!simulating.get()) throw new IllegalStateException("Cannot simulate time unless in simulation mode");
        simulatedCurrTimeMs.set(simulatedCurrTimeMs.get() + ms);
    }
    
    public static boolean isThreadWaiting(Thread t) {
        if(!simulating.get()) throw new IllegalStateException("Must be in simulation mode");
        AtomicLong time;
        synchronized(sleepTimesLock) {
            time = threadSleepTimes.get(t);
        }
        return !t.isAlive() || time!=null && currentTimeMillis() < time.longValue();
    }    
}
