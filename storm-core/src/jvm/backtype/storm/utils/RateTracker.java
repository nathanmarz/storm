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

import java.util.*;

/**
 * This class is a utility to track the rate.
 */
public class RateTracker{
    /* number of slides to keep in the history. */
    public final int _numOfSlides; // number of slides to keep in the history

    public final int _slideSizeInMils;
    private final long[] _histograms;// an array storing the number of element for each time slide.

    private int _currentValidSlideNum;

    private static Timer _timer = new Timer();

    /**
     * @param validTimeWindowInMils events that happened before validTimeWindowInMils are not considered
     *                        when reporting the rate.
     * @param numOfSlides the number of time sildes to divide validTimeWindows. The more slides,
     *                    the smother the reported results will be.
     */
    public RateTracker(int validTimeWindowInMils, int numOfSlides) {
        this(validTimeWindowInMils, numOfSlides, false);
    }

    /**
     * Constructor
     * @param validTimeWindowInMils events that happened before validTimeWindow are not considered
     *                        when reporting the rate.
     * @param numOfSlides the number of time sildes to divide validTimeWindows. The more slides,
     *                    the smother the reported results will be.
     * @param simulate set true if it use simulated time rather than system time for testing purpose.
     */
    public RateTracker(int validTimeWindowInMils, int numOfSlides, boolean simulate ){
        _numOfSlides = Math.max(numOfSlides, 1);
        _slideSizeInMils = validTimeWindowInMils / _numOfSlides;
        if (_slideSizeInMils < 1 ) {
            throw new IllegalArgumentException("Illeggal argument for RateTracker");
        }
        assert(_slideSizeInMils > 1);
        _histograms = new long[_numOfSlides];
        Arrays.fill(_histograms,0L);
        if(!simulate) {
            _timer.scheduleAtFixedRate(new Fresher(), _slideSizeInMils, _slideSizeInMils);
        }
        _currentValidSlideNum = 1;
    }

    /**
     * Notify the tracker upon new arrivals
     *
     * @param count number of arrivals
     */
    public void notify(long count) {
        _histograms[_histograms.length-1]+=count;
    }

    /**
     * Return the average rate in slides.
     *
     * @return the average rate
     */
    public final float reportRate() {
        long sum = 0;
        long duration = _currentValidSlideNum * _slideSizeInMils;
        for(int i=_numOfSlides - _currentValidSlideNum; i < _numOfSlides; i++ ){
            sum += _histograms[i];
        }

        return sum / (float) duration * 1000;
    }

    public final void forceUpdateSlides(int numToEclipse) {

        for(int i=0; i< numToEclipse; i++) {
            updateSlides();
        }

    }

    private void updateSlides(){

        for (int i = 0; i < _numOfSlides - 1; i++) {
            _histograms[i] = _histograms[i + 1];
        }

        _histograms[_histograms.length - 1] = 0;

        _currentValidSlideNum = Math.min(_currentValidSlideNum + 1, _numOfSlides);
    }

    private class Fresher extends TimerTask {
        public void run () {
            updateSlides();
        }
    }


}
