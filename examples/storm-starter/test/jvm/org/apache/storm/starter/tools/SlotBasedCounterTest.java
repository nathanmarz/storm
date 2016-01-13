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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class SlotBasedCounterTest {

  private static final int ANY_NUM_SLOTS = 1;
  private static final int ANY_SLOT = 0;
  private static final Object ANY_OBJECT = "ANY_OBJECT";

  @DataProvider
  public Object[][] illegalNumSlotsData() {
    return new Object[][]{ { -10 }, { -3 }, { -2 }, { -1 }, { 0 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalNumSlotsData")
  public void negativeOrZeroNumSlotsShouldThrowIAE(int numSlots) {
    new SlotBasedCounter<Object>(numSlots);
  }

  @DataProvider
  public Object[][] legalNumSlotsData() {
    return new Object[][]{ { 1 }, { 2 }, { 3 }, { 20 } };
  }

  @Test(dataProvider = "legalNumSlotsData")
  public void positiveNumSlotsShouldBeOk(int numSlots) {
    new SlotBasedCounter<Object>(numSlots);
  }

  @Test
  public void newInstanceShouldHaveEmptyCounts() {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(ANY_NUM_SLOTS);

    // when
    Map<Object, Long> counts = counter.getCounts();

    // then
    assertThat(counts).isEmpty();
  }

  @Test
  public void shouldReturnNonEmptyCountsWhenAtLeastOneObjectWasCounted() {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(ANY_NUM_SLOTS);
    counter.incrementCount(ANY_OBJECT, ANY_SLOT);

    // when
    Map<Object, Long> counts = counter.getCounts();

    // then
    assertThat(counts).isNotEmpty();

    // additional tests that go beyond what this test is primarily about
    assertThat(counts.size()).isEqualTo(1);
    assertThat(counts.get(ANY_OBJECT)).isEqualTo(1);
  }

  @DataProvider
  public Object[][] incrementCountData() {
    return new Object[][]{ { new String[]{ "foo", "bar" }, new int[]{ 3, 2 } } };
  }

  @Test(dataProvider = "incrementCountData")
  public void shouldIncrementCount(Object[] objects, int[] expCounts) {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(ANY_NUM_SLOTS);

    // when
    for (int i = 0; i < objects.length; i++) {
      Object obj = objects[i];
      int numIncrements = expCounts[i];
      for (int j = 0; j < numIncrements; j++) {
        counter.incrementCount(obj, ANY_SLOT);
      }
    }

    // then
    for (int i = 0; i < objects.length; i++) {
      assertThat(counter.getCount(objects[i], ANY_SLOT)).isEqualTo(expCounts[i]);
    }
    assertThat(counter.getCount("nonexistentObject", ANY_SLOT)).isEqualTo(0);
  }

  @Test
  public void shouldReturnZeroForNonexistentObject() {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(ANY_NUM_SLOTS);

    // when
    counter.incrementCount("somethingElse", ANY_SLOT);

    // then
    assertThat(counter.getCount("nonexistentObject", ANY_SLOT)).isEqualTo(0);
  }

  @Test
  public void shouldIncrementCountOnlyOneSlotAtATime() {
    // given
    int numSlots = 3;
    Object obj = Long.valueOf(10);
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(numSlots);

    // when (empty)
    // then
    assertThat(counter.getCount(obj, 0)).isEqualTo(0);
    assertThat(counter.getCount(obj, 1)).isEqualTo(0);
    assertThat(counter.getCount(obj, 2)).isEqualTo(0);

    // when
    counter.incrementCount(obj, 1);

    // then
    assertThat(counter.getCount(obj, 0)).isEqualTo(0);
    assertThat(counter.getCount(obj, 1)).isEqualTo(1);
    assertThat(counter.getCount(obj, 2)).isEqualTo(0);
  }

  @Test
  public void wipeSlotShouldSetAllCountsInSlotToZero() {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(ANY_NUM_SLOTS);
    Object countWasOne = "countWasOne";
    Object countWasThree = "countWasThree";
    counter.incrementCount(countWasOne, ANY_SLOT);
    counter.incrementCount(countWasThree, ANY_SLOT);
    counter.incrementCount(countWasThree, ANY_SLOT);
    counter.incrementCount(countWasThree, ANY_SLOT);

    // when
    counter.wipeSlot(ANY_SLOT);

    // then
    assertThat(counter.getCount(countWasOne, ANY_SLOT)).isEqualTo(0);
    assertThat(counter.getCount(countWasThree, ANY_SLOT)).isEqualTo(0);
  }

  @Test
  public void wipeZerosShouldRemoveAnyObjectsWithZeroTotalCount() {
    // given
    SlotBasedCounter<Object> counter = new SlotBasedCounter<Object>(2);
    int wipeSlot = 0;
    int otherSlot = 1;
    Object willBeRemoved = "willBeRemoved";
    Object willContinueToBeTracked = "willContinueToBeTracked";
    counter.incrementCount(willBeRemoved, wipeSlot);
    counter.incrementCount(willContinueToBeTracked, wipeSlot);
    counter.incrementCount(willContinueToBeTracked, otherSlot);

    // when
    counter.wipeSlot(wipeSlot);
    counter.wipeZeros();

    // then
    assertThat(counter.getCounts()).doesNotContainKey(willBeRemoved);
    assertThat(counter.getCounts()).containsKey(willContinueToBeTracked);
  }
}
