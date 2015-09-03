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
package backtype.storm.utils;

import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;

/**
 * Unit test for RateTracker
 */
public class RateTrackerTest extends TestCase {

    @Test
    public void testEclipsedAllWindows() {
        RateTracker rt = new RateTracker(10000, 10, true);
        rt.notify(10);
        rt.forceUpdateSlides(10);
        assert (rt.reportRate() == 0);
    }

    @Test
    public void testEclipsedOneWindow() {
        RateTracker rt = new RateTracker(10000, 10, true);
        rt.notify(1);
        float r1 = rt.reportRate();
        rt.forceUpdateSlides(1);
        rt.notify(1);
        float r2 = rt.reportRate();

        System.out.format("r1:%f, r2:%f\n", r1, r2);

        assert (r1 == r2);
    }

    @Test
    public void testEclipsedNineWindows() {
        RateTracker rt = new RateTracker(10000, 10, true);
        rt.notify(1);
        float r1 = rt.reportRate();
        rt.forceUpdateSlides(9);
        rt.notify(9);
        float r2 = rt.reportRate();

        assert (r1 == r2);
    }
}