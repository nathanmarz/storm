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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.jmock.lib.concurrent.Blitzer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class RankingsTest {

  private static final int ANY_TOPN = 42;
  private static final Rankable ANY_RANKABLE = new RankableObjectWithFields("someObject", ANY_TOPN);
  private static final Rankable ZERO = new RankableObjectWithFields("ZERO_COUNT", 0);
  private static final Rankable A = new RankableObjectWithFields("A", 1);
  private static final Rankable B = new RankableObjectWithFields("B", 2);
  private static final Rankable C = new RankableObjectWithFields("C", 3);
  private static final Rankable D = new RankableObjectWithFields("D", 4);
  private static final Rankable E = new RankableObjectWithFields("E", 5);
  private static final Rankable F = new RankableObjectWithFields("F", 6);
  private static final Rankable G = new RankableObjectWithFields("G", 7);
  private static final Rankable H = new RankableObjectWithFields("H", 8);

  @DataProvider
  public Object[][] illegalTopNData() {
    return new Object[][]{ { 0 }, { -1 }, { -2 }, { -10 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalTopNData")
  public void constructorWithNegativeOrZeroTopNShouldThrowIAE(int topN) {
    new Rankings(topN);
  }

  @DataProvider
  public Object[][] copyRankingsData() {
    return new Object[][]{ { 5, Lists.newArrayList(A, B, C) }, { 2, Lists.newArrayList(A, B, C, D) },
        { 1, Lists.newArrayList() }, { 1, Lists.newArrayList(A) }, { 1, Lists.newArrayList(A, B) } };
  }

  @Test(dataProvider = "copyRankingsData")
  public void copyConstructorShouldReturnCopy(int topN, List<Rankable> rankables) {
    // given
    Rankings rankings = new Rankings(topN);
    for (Rankable r : rankables) {
      rankings.updateWith(r);
    }

    // when
    Rankings copy = new Rankings(rankings);

    // then
    assertThat(copy.maxSize()).isEqualTo(rankings.maxSize());
    assertThat(copy.getRankings()).isEqualTo(rankings.getRankings());
  }

  @DataProvider
  public Object[][] defensiveCopyRankingsData() {
    return new Object[][]{ { 5, Lists.newArrayList(A, B, C), Lists.newArrayList(D) }, { 2, Lists.newArrayList(A, B, C,
        D), Lists.newArrayList(E, F) }, { 1, Lists.newArrayList(), Lists.newArrayList(A) }, { 1, Lists.newArrayList(A),
        Lists.newArrayList(B) }, { 1, Lists.newArrayList(ZERO), Lists.newArrayList(B) }, { 1, Lists.newArrayList(ZERO),
        Lists.newArrayList() } };
  }

  @Test(dataProvider = "defensiveCopyRankingsData")
  public void copyConstructorShouldReturnDefensiveCopy(int topN, List<Rankable> rankables, List<Rankable> changes) {
    // given
    Rankings original = new Rankings(topN);
    for (Rankable r : rankables) {
      original.updateWith(r);
    }
    int expSize = original.size();
    List<Rankable> expRankings = original.getRankings();

    // when
    Rankings copy = new Rankings(original);
    for (Rankable r : changes) {
      copy.updateWith(r);
    }

    // then
    assertThat(original.size()).isEqualTo(expSize);
    assertThat(original.getRankings()).isEqualTo(expRankings);
  }

  @DataProvider
  public Object[][] legalTopNData() {
    return new Object[][]{ { 1 }, { 2 }, { 1000 }, { 1000000 } };
  }

  @Test(dataProvider = "legalTopNData")
  public void constructorWithPositiveTopNShouldBeOk(int topN) {
    // given/when
    Rankings rankings = new Rankings(topN);

    // then
    assertThat(rankings.maxSize()).isEqualTo(topN);
  }

  @Test
  public void shouldHaveDefaultConstructor() {
    new Rankings();
  }

  @Test
  public void defaultConstructorShouldSetPositiveTopN() {
    // given/when
    Rankings rankings = new Rankings();

    // then
    assertThat(rankings.maxSize()).isGreaterThan(0);
  }

  @DataProvider
  public Object[][] rankingsGrowData() {
    return new Object[][]{ { 2, Lists.newArrayList(new RankableObjectWithFields("A", 1), new RankableObjectWithFields(
        "B", 2), new RankableObjectWithFields("C", 3)) }, { 2, Lists.newArrayList(new RankableObjectWithFields("A", 1),
        new RankableObjectWithFields("B", 2), new RankableObjectWithFields("C", 3), new RankableObjectWithFields("D",
        4)) } };
  }

  @Test(dataProvider = "rankingsGrowData")
  public void sizeOfRankingsShouldNotGrowBeyondTopN(int topN, List<Rankable> rankables) {
    // sanity check of the provided test data
    assertThat(rankables.size()).overridingErrorMessage(
        "The supplied test data is not correct: the number of rankables <%d> should be greater than <%d>",
        rankables.size(), topN).isGreaterThan(topN);

    // given
    Rankings rankings = new Rankings(topN);

    // when
    for (Rankable r : rankables) {
      rankings.updateWith(r);
    }

    // then
    assertThat(rankings.size()).isLessThanOrEqualTo(rankings.maxSize());
  }

  @DataProvider
  public Object[][] simulatedRankingsData() {
    return new Object[][]{ { Lists.newArrayList(A), Lists.newArrayList(A) }, { Lists.newArrayList(B, D, A, C),
        Lists.newArrayList(D, C, B, A) }, { Lists.newArrayList(B, F, A, C, D, E), Lists.newArrayList(F, E, D, C, B,
        A) }, { Lists.newArrayList(G, B, F, A, C, D, E, H), Lists.newArrayList(H, G, F, E, D, C, B, A) } };
  }

  @Test(dataProvider = "simulatedRankingsData")
  public void shouldCorrectlyRankWhenUpdatedWithRankables(List<Rankable> unsorted, List<Rankable> expSorted) {
    // given
    Rankings rankings = new Rankings(unsorted.size());

    // when
    for (Rankable r : unsorted) {
      rankings.updateWith(r);
    }

    // then
    assertThat(rankings.getRankings()).isEqualTo(expSorted);
  }

  @Test(dataProvider = "simulatedRankingsData")
  public void shouldCorrectlyRankWhenEmptyAndUpdatedWithOtherRankings(List<Rankable> unsorted,
      List<Rankable> expSorted) {
    // given
    Rankings rankings = new Rankings(unsorted.size());
    Rankings otherRankings = new Rankings(rankings.maxSize());
    for (Rankable r : unsorted) {
      otherRankings.updateWith(r);
    }

    // when
    rankings.updateWith(otherRankings);

    // then
    assertThat(rankings.getRankings()).isEqualTo(expSorted);
  }

  @Test(dataProvider = "simulatedRankingsData")
  public void shouldCorrectlyRankWhenUpdatedWithEmptyOtherRankings(List<Rankable> unsorted, List<Rankable> expSorted) {
    // given
    Rankings rankings = new Rankings(unsorted.size());
    for (Rankable r : unsorted) {
      rankings.updateWith(r);
    }
    Rankings emptyRankings = new Rankings(ANY_TOPN);

    // when
    rankings.updateWith(emptyRankings);

    // then
    assertThat(rankings.getRankings()).isEqualTo(expSorted);
  }

  @DataProvider
  public Object[][] simulatedRankingsAndOtherRankingsData() {
    return new Object[][]{ { Lists.newArrayList(A), Lists.newArrayList(A), Lists.newArrayList(A) },
        { Lists.newArrayList(A, C), Lists.newArrayList(B, D), Lists.newArrayList(D, C, B, A) }, { Lists.newArrayList(B,
        F, A), Lists.newArrayList(C, D, E), Lists.newArrayList(F, E, D, C, B, A) }, { Lists.newArrayList(G, B, F, A, C),
        Lists.newArrayList(D, E, H), Lists.newArrayList(H, G, F, E, D, C, B, A) } };
  }

  @Test(dataProvider = "simulatedRankingsAndOtherRankingsData")
  public void shouldCorrectlyRankWhenNotEmptyAndUpdatedWithOtherRankings(List<Rankable> unsorted,
      List<Rankable> unsortedForOtherRankings, List<Rankable> expSorted) {
    // given
    Rankings rankings = new Rankings(expSorted.size());
    for (Rankable r : unsorted) {
      rankings.updateWith(r);
    }
    Rankings otherRankings = new Rankings(unsortedForOtherRankings.size());
    for (Rankable r : unsortedForOtherRankings) {
      otherRankings.updateWith(r);
    }

    // when
    rankings.updateWith(otherRankings);

    // then
    assertThat(rankings.getRankings()).isEqualTo(expSorted);
  }

  @DataProvider
  public Object[][] duplicatesData() {
    Rankable A1 = new RankableObjectWithFields("A", 1);
    Rankable A2 = new RankableObjectWithFields("A", 2);
    Rankable A3 = new RankableObjectWithFields("A", 3);
    return new Object[][]{ { Lists.newArrayList(ANY_RANKABLE, ANY_RANKABLE, ANY_RANKABLE) }, { Lists.newArrayList(A1,
        A2, A3) }, };
  }

  @Test(dataProvider = "duplicatesData")
  public void shouldNotRankDuplicateObjectsMoreThanOnce(List<Rankable> duplicates) {
    // given
    Rankings rankings = new Rankings(duplicates.size());

    // when
    for (Rankable r : duplicates) {
      rankings.updateWith(r);
    }

    // then
    assertThat(rankings.size()).isEqualTo(1);
  }

  @DataProvider
  public Object[][] removeZeroRankingsData() {
    return new Object[][]{ { Lists.newArrayList(A, ZERO), Lists.newArrayList(A) }, { Lists.newArrayList(A),
        Lists.newArrayList(A) }, { Lists.newArrayList(ZERO, A), Lists.newArrayList(A) }, { Lists.newArrayList(ZERO),
        Lists.newArrayList() }, { Lists.newArrayList(ZERO, new RankableObjectWithFields("ZERO2", 0)),
        Lists.newArrayList() }, { Lists.newArrayList(B, ZERO, new RankableObjectWithFields("ZERO2", 0), D,
        new RankableObjectWithFields("ZERO3", 0), new RankableObjectWithFields("ZERO4", 0), C), Lists.newArrayList(D, C,
        B) }, { Lists.newArrayList(A, ZERO, B), Lists.newArrayList(B, A) } };
  }

  @Test(dataProvider = "removeZeroRankingsData")
  public void shouldRemoveZeroCounts(List<Rankable> unsorted, List<Rankable> expSorted) {
    // given
    Rankings rankings = new Rankings(unsorted.size());
    for (Rankable r : unsorted) {
      rankings.updateWith(r);
    }

    // when
    rankings.pruneZeroCounts();

    // then
    assertThat(rankings.getRankings()).isEqualTo(expSorted);
  }

  @Test
  public void updatingWithNewRankablesShouldBeThreadSafe() throws InterruptedException {
    // given
    final List<Rankable> entries = ImmutableList.of(A, B, C, D);
    final Rankings rankings = new Rankings(entries.size());

    // We are capturing exceptions thrown in Blitzer's child threads into this data structure so that we can properly
    // pass/fail this test.  The reason is that Blitzer doesn't report exceptions, which is a known bug in Blitzer
    // (JMOCK-263).  See https://github.com/jmock-developers/jmock-library/issues/22 for more information.
    final List<Exception> exceptions = Lists.newArrayList();
    Blitzer blitzer = new Blitzer(1000);

    // when
    blitzer.blitz(new Runnable() {
      public void run() {
        for (Rankable r : entries) {
          try {
            rankings.updateWith(r);
          }
          catch (RuntimeException e) {
            synchronized(exceptions) {
              exceptions.add(e);
            }
          }
        }
      }
    });
    blitzer.shutdown();

    // then
    //
    if (!exceptions.isEmpty()) {
      for (Exception e : exceptions) {
        System.err.println(Throwables.getStackTraceAsString(e));
      }
    }
    assertThat(exceptions).isEmpty();
  }

  @Test(dataProvider = "copyRankingsData")
  public void copyShouldReturnCopy(int topN, List<Rankable> rankables) {
    // given
    Rankings rankings = new Rankings(topN);
    for (Rankable r : rankables) {
      rankings.updateWith(r);
    }

    // when
    Rankings copy = rankings.copy();

    // then
    assertThat(copy.maxSize()).isEqualTo(rankings.maxSize());
    assertThat(copy.getRankings()).isEqualTo(rankings.getRankings());
  }

  @Test(dataProvider = "defensiveCopyRankingsData")
  public void copyShouldReturnDefensiveCopy(int topN, List<Rankable> rankables, List<Rankable> changes) {
    // given
    Rankings original = new Rankings(topN);
    for (Rankable r : rankables) {
      original.updateWith(r);
    }
    int expSize = original.size();
    List<Rankable> expRankings = original.getRankings();

    // when
    Rankings copy = original.copy();
    for (Rankable r : changes) {
      copy.updateWith(r);
    }
    copy.pruneZeroCounts();

    // then
    assertThat(original.size()).isEqualTo(expSize);
    assertThat(original.getRankings()).isEqualTo(expRankings);
  }

}
