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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metric.internal;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;
import junit.framework.TestCase;
import static org.junit.Assert.*;

/**
 * Unit test for LatencyStatAndMetric
 */
public class LatencyStatAndMetricTest extends TestCase {
    final long TEN_MIN = 10 * 60 * 1000;
    final long THIRTY_SEC = 30 * 1000;
    final long THREE_HOUR = 3 * 60 * 60 * 1000;
    final long ONE_DAY = 24 * 60 * 60 * 1000;

    @Test
    public void testBasic() {
        long time = 0l;
        LatencyStatAndMetric lat = new LatencyStatAndMetric(10, time);
        while (time < TEN_MIN) {
            lat.record(100);
            time += THIRTY_SEC;
            assertEquals(100.0, ((Double)lat.getValueAndReset(time)).doubleValue(), 0.01);
        }

        Map<String, Double> found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(100.0, found.get("600").doubleValue(), 0.01);
        assertEquals(100.0, found.get("10800").doubleValue(), 0.01);
        assertEquals(100.0, found.get("86400").doubleValue(), 0.01);
        assertEquals(100.0, found.get(":all-time").doubleValue(), 0.01);

        while (time < THREE_HOUR) {
            lat.record(200);
            time += THIRTY_SEC;
            assertEquals(200.0, ((Double)lat.getValueAndReset(time)).doubleValue(), 0.01);
        }

        double expected = ((100.0 * TEN_MIN/THIRTY_SEC) + (200.0 * (THREE_HOUR - TEN_MIN)/THIRTY_SEC)) /
                          (THREE_HOUR/THIRTY_SEC);
        found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(200.0, found.get("600").doubleValue(), 0.01); //flushed the buffers completely
        assertEquals(expected, found.get("10800").doubleValue(), 0.01);
        assertEquals(expected, found.get("86400").doubleValue(), 0.01);
        assertEquals(expected, found.get(":all-time").doubleValue(), 0.01);

        while (time < ONE_DAY) {
            lat.record(300);
            time += THIRTY_SEC;
            assertEquals(300.0, ((Double)lat.getValueAndReset(time)).doubleValue(), 0.01);
        }

        expected = ((100.0 * TEN_MIN/THIRTY_SEC) + (200.0 * (THREE_HOUR - TEN_MIN)/THIRTY_SEC) + (300.0 * (ONE_DAY - THREE_HOUR)/THIRTY_SEC)) /
                          (ONE_DAY/THIRTY_SEC);
        found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(300.0, found.get("600").doubleValue(), 0.01); //flushed the buffers completely
        assertEquals(300.0, found.get("10800").doubleValue(), 0.01);
        assertEquals(expected, found.get("86400").doubleValue(), 0.01);
        assertEquals(expected, found.get(":all-time").doubleValue(), 0.01);
    }
}
