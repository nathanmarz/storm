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
package org.apache.storm.metrics.hdrhistogram;

import org.apache.storm.metric.api.IMetric;
import org.HdrHistogram.Histogram;

/**
 * A metric wrapping an HdrHistogram.
 */
public class HistogramMetric implements IMetric {
    private final Histogram _histo;


    public HistogramMetric(final int numberOfSignificantValueDigits) {
        this(null, numberOfSignificantValueDigits);
    }

    public HistogramMetric(Long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(null, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * (From the Constructor of Histogram)
     * Construct a Histogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestDiscernibleValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestDiscernibleValue would be 1000.
     *
     * @param lowestDiscernibleValue         The lowest value that can be discerned (distinguished from 0) by the
     *                                       histogram. Must be a positive integer that is {@literal >=} 1. May be
     *                                       internally rounded down to nearest power of 2
     *                                       (if null 1 is used).
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} (2 * lowestDiscernibleValue).
     *                                       (if null 2 * lowestDiscernibleValue is used and auto-resize is enabled)
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public HistogramMetric(Long lowestDiscernibleValue, Long highestTrackableValue,
                     final int numberOfSignificantValueDigits) {
        boolean autoResize = false;
        if (lowestDiscernibleValue == null) lowestDiscernibleValue = 1L;
        if (highestTrackableValue == null) {
            highestTrackableValue = 2 * lowestDiscernibleValue;
            autoResize = true;
        }
        _histo = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        if (autoResize) _histo.setAutoResize(true);
    }

    public void recordValue(long val) {
        _histo.recordValue(val);
    }

    @Override
    public Object getValueAndReset() {
          Histogram copy = _histo.copy();
          _histo.reset();
          return copy;
    }
}
