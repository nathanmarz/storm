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
package org.apache.storm.starter.tools;

import org.apache.storm.utils.Time;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class NthLastModifiedTimeTrackerTest {

  private static final int ANY_NUM_TIMES_TO_TRACK = 3;
  private static final int MILLIS_IN_SEC = 1000;

  @DataProvider
  public Object[][] illegalNumTimesData() {
    return new Object[][]{ { -10 }, { -3 }, { -2 }, { -1 }, { 0 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalNumTimesData")
  public void negativeOrZeroNumTimesToTrackShouldThrowIAE(int numTimesToTrack) {
    new NthLastModifiedTimeTracker(numTimesToTrack);
  }

  @DataProvider
  public Object[][] legalNumTimesData() {
    return new Object[][]{ { 1 }, { 2 }, { 3 }, { 20 } };
  }

  @Test(dataProvider = "legalNumTimesData")
  public void positiveNumTimesToTrackShouldBeOk(int numTimesToTrack) {
    new NthLastModifiedTimeTracker(numTimesToTrack);
  }

  @DataProvider
  public Object[][] whenNotYetMarkedAsModifiedData() {
    return new Object[][]{ { 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }, { 8 }, { 10 } };
  }

  @Test(dataProvider = "whenNotYetMarkedAsModifiedData")
  public void shouldReturnCorrectModifiedTimeEvenWhenNotYetMarkedAsModified(int secondsToAdvance) {
    // given
    Time.startSimulating();
    NthLastModifiedTimeTracker tracker = new NthLastModifiedTimeTracker(ANY_NUM_TIMES_TO_TRACK);

    // when
    advanceSimulatedTimeBy(secondsToAdvance);
    int seconds = tracker.secondsSinceOldestModification();

    // then
    assertThat(seconds).isEqualTo(secondsToAdvance);

    // cleanup
    Time.stopSimulating();
  }

  @DataProvider
  public Object[][] simulatedTrackerIterations() {
    return new Object[][]{ { 1, new int[]{ 0, 1 }, new int[]{ 0, 0 } }, { 1, new int[]{ 0, 2 }, new int[]{ 0, 0 } },
        { 2, new int[]{ 2, 2 }, new int[]{ 2, 2 } }, { 2, new int[]{ 0, 4 }, new int[]{ 0, 4 } },
        { 1, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 0, 0, 0, 0, 0, 0, 0 } },
        { 1, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 0, 0, 0, 0, 0, 0, 0 } },
        { 2, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 1, 1, 1, 1, 1, 1, 1 } },
        { 2, new int[]{ 2, 2, 2, 2, 2, 2, 2 }, new int[]{ 2, 2, 2, 2, 2, 2, 2 } },
        { 2, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 1, 2, 3, 4, 5, 6, 7 } },
        { 3, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 1, 2, 2, 2, 2, 2, 2 } },
        { 3, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 1, 3, 5, 7, 9, 11, 13 } },
        { 3, new int[]{ 2, 2, 2, 2, 2, 2, 2 }, new int[]{ 2, 4, 4, 4, 4, 4, 4 } },
        { 4, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 1, 2, 3, 3, 3, 3, 3 } },
        { 4, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 1, 3, 6, 9, 12, 15, 18 } },
        { 4, new int[]{ 2, 2, 2, 2, 2, 2, 2 }, new int[]{ 2, 4, 6, 6, 6, 6, 6 } },
        { 5, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 1, 2, 3, 4, 4, 4, 4 } },
        { 5, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 1, 3, 6, 10, 14, 18, 22 } },
        { 5, new int[]{ 2, 2, 2, 2, 2, 2, 2 }, new int[]{ 2, 4, 6, 8, 8, 8, 8 } },
        { 6, new int[]{ 1, 1, 1, 1, 1, 1, 1 }, new int[]{ 1, 2, 3, 4, 5, 5, 5 } },
        { 6, new int[]{ 1, 2, 3, 4, 5, 6, 7 }, new int[]{ 1, 3, 6, 10, 15, 20, 25 } },
        { 6, new int[]{ 2, 2, 2, 2, 2, 2, 2 }, new int[]{ 2, 4, 6, 8, 10, 10, 10 } },
        { 3, new int[]{ 1, 2, 3 }, new int[]{ 1, 3, 5 } } };
  }

  @Test(dataProvider = "simulatedTrackerIterations")
  public void shouldReturnCorrectModifiedTimeWhenMarkedAsModified(int numTimesToTrack,
      int[] secondsToAdvancePerIteration, int[] expLastModifiedTimes) {
    // given
    Time.startSimulating();
    NthLastModifiedTimeTracker tracker = new NthLastModifiedTimeTracker(numTimesToTrack);

    int[] modifiedTimes = new int[expLastModifiedTimes.length];

    // when
    int i = 0;
    for (int secondsToAdvance : secondsToAdvancePerIteration) {
      advanceSimulatedTimeBy(secondsToAdvance);
      tracker.markAsModified();
      modifiedTimes[i] = tracker.secondsSinceOldestModification();
      i++;
    }

    // then
    assertThat(modifiedTimes).isEqualTo(expLastModifiedTimes);

    // cleanup
    Time.stopSimulating();
  }

  private void advanceSimulatedTimeBy(int seconds) {
    Time.advanceTime(seconds * MILLIS_IN_SEC);
  }
}
