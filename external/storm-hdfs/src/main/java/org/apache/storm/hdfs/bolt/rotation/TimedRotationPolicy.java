/*
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
package org.apache.storm.hdfs.bolt.rotation;

import org.apache.storm.tuple.Tuple;

public class TimedRotationPolicy implements FileRotationPolicy {

    public static enum TimeUnit {

        SECONDS((long)1000),
        MINUTES((long)1000*60),
        HOURS((long)1000*60*60),
        DAYS((long)1000*60*60*24);

        private long milliSeconds;

        private TimeUnit(long milliSeconds){
            this.milliSeconds = milliSeconds;
        }

        public long getMilliSeconds(){
            return milliSeconds;
        }
    }

    private long interval;

    public TimedRotationPolicy(float count, TimeUnit units){
        this.interval = (long)(count * units.getMilliSeconds());
    }


    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple  The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    @Override
    public boolean mark(Tuple tuple, long offset) {
        return false;
    }

    /**
     * Called after the HdfsBolt rotates a file.
     */
    @Override
    public void reset() {

    }

    public long getInterval(){
        return this.interval;
    }
}
