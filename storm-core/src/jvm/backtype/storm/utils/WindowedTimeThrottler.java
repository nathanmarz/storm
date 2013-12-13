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

public class WindowedTimeThrottler {
    long _windowMillis;
    int _maxAmt;
    long _windowStartTime;
    int _windowEvents = 0;
    
    public WindowedTimeThrottler(Number windowMillis, Number maxAmt) {
        _windowMillis = windowMillis.longValue();
        _maxAmt = maxAmt.intValue();
        _windowStartTime = System.currentTimeMillis();
    }
    
    public boolean isThrottled() {
        resetIfNecessary();
        return _windowEvents >= _maxAmt;
    }
    
    //returns void if the event should continue, false if the event should not be done
    public void markEvent() {
        resetIfNecessary();
        _windowEvents++;
        
    }
    
    private void resetIfNecessary() {
        long now = System.currentTimeMillis();
        if(now - _windowStartTime > _windowMillis) {
            _windowStartTime = now;
            _windowEvents = 0;
        }
    }
}
