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

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TimerTask;

public class Timer {

    public interface KillFunc {
        public void onKill(Throwable throwable);
    }

    public static class StormTimerTask extends TimerTask {

        PriorityQueue queue = new PriorityQueue(10, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return 0;
            }
        });

        @Override
        public void run() {

        }
    }

    public TimerTask mkTimer(String name) {

    }

}
