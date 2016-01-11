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

import org.apache.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class RankableObjectWithFieldsTest {

  private static final Object ANY_OBJECT = new Object();
  private static final long ANY_COUNT = 271;
  private static final String ANY_FIELD = "someAdditionalField";
  private static final int GREATER_THAN = 1;
  private static final int EQUAL_TO = 0;
  private static final int SMALLER_THAN = -1;

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void constructorWithNullObjectAndNoFieldsShouldThrowIAE() {
    new RankableObjectWithFields(null, ANY_COUNT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void constructorWithNullObjectAndFieldsShouldThrowIAE() {
    Object someAdditionalField = new Object();
    new RankableObjectWithFields(null, ANY_COUNT, someAdditionalField);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void constructorWithNegativeCountAndNoFieldsShouldThrowIAE() {
    new RankableObjectWithFields(ANY_OBJECT, -1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void constructorWithNegativeCountAndFieldsShouldThrowIAE() {
    Object someAdditionalField = new Object();
    new RankableObjectWithFields(ANY_OBJECT, -1, someAdditionalField);
  }

  @Test
  public void shouldBeEqualToItself() {
    RankableObjectWithFields r = new RankableObjectWithFields(ANY_OBJECT, ANY_COUNT);
    assertThat(r).isEqualTo(r);
  }

  @DataProvider
  public Object[][] otherClassesData() {
    return new Object[][]{ { new String("foo") }, { new Object() }, { Integer.valueOf(4) }, { Lists.newArrayList(7, 8,
        9) } };
  }

  @Test(dataProvider = "otherClassesData")
  public void shouldNotBeEqualToInstancesOfOtherClasses(Object notARankable) {
    RankableObjectWithFields r = new RankableObjectWithFields(ANY_OBJECT, ANY_COUNT);
    assertFalse(r.equals(notARankable), r + " is equal to " + notARankable + " but it should not be");
  }

  @DataProvider
  public Object[][] falseDuplicatesData() {
    return new Object[][]{ { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 1) },
        { new RankableObjectWithFields("foo", 1), new RankableObjectWithFields("Foo", 1) },
        { new RankableObjectWithFields("foo", 1), new RankableObjectWithFields("FOO", 1) },
        { new RankableObjectWithFields("foo", 1), new RankableObjectWithFields("bar", 1) },
        { new RankableObjectWithFields("", 0), new RankableObjectWithFields("", 1) }, { new RankableObjectWithFields("",
        1), new RankableObjectWithFields("bar", 1) } };
  }

  @Test(dataProvider = "falseDuplicatesData")
  public void shouldNotBeEqualToFalseDuplicates(RankableObjectWithFields r, RankableObjectWithFields falseDuplicate) {
    assertFalse(r.equals(falseDuplicate), r + " is equal to " + falseDuplicate + " but it should not be");
  }

  @Test(dataProvider = "falseDuplicatesData")
  public void shouldHaveDifferentHashCodeThanFalseDuplicates(RankableObjectWithFields r,
      RankableObjectWithFields falseDuplicate) {
    assertThat(r.hashCode()).isNotEqualTo(falseDuplicate.hashCode());
  }

  @DataProvider
  public Object[][] trueDuplicatesData() {
    return new Object[][]{ { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 0) },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 0, "someOtherField") },
        { new RankableObjectWithFields("foo", 0, "someField"), new RankableObjectWithFields("foo", 0,
            "someOtherField") } };
  }

  @Test(dataProvider = "trueDuplicatesData")
  public void shouldBeEqualToTrueDuplicates(RankableObjectWithFields r, RankableObjectWithFields trueDuplicate) {
    assertTrue(r.equals(trueDuplicate), r + " is not equal to " + trueDuplicate + " but it should be");
  }

  @Test(dataProvider = "trueDuplicatesData")
  public void shouldHaveSameHashCodeAsTrueDuplicates(RankableObjectWithFields r,
      RankableObjectWithFields trueDuplicate) {
    assertThat(r.hashCode()).isEqualTo(trueDuplicate.hashCode());
  }

  @DataProvider
  public Object[][] compareToData() {
    return new Object[][]{ { new RankableObjectWithFields("foo", 1000), new RankableObjectWithFields("foo", 0),
        GREATER_THAN }, { new RankableObjectWithFields("foo", 1), new RankableObjectWithFields("foo", 0),
        GREATER_THAN }, { new RankableObjectWithFields("foo", 1000), new RankableObjectWithFields("bar", 0),
        GREATER_THAN }, { new RankableObjectWithFields("foo", 1), new RankableObjectWithFields("bar", 0),
        GREATER_THAN }, { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 0), EQUAL_TO },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("bar", 0), EQUAL_TO },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 1000), SMALLER_THAN },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("foo", 1), SMALLER_THAN },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("bar", 1), SMALLER_THAN },
        { new RankableObjectWithFields("foo", 0), new RankableObjectWithFields("bar", 1000), SMALLER_THAN }, };
  }

  @Test(dataProvider = "compareToData")
  public void verifyCompareTo(RankableObjectWithFields first, RankableObjectWithFields second, int expCompareToValue) {
    assertThat(first.compareTo(second)).isEqualTo(expCompareToValue);
  }

  @DataProvider
  public Object[][] toStringData() {
    return new Object[][]{ { new String("foo"), 0L }, { new String("BAR"), 8L } };
  }

  @Test(dataProvider = "toStringData")
  public void toStringShouldContainStringRepresentationsOfObjectAndCount(Object obj, long count) {
    // given
    RankableObjectWithFields r = new RankableObjectWithFields(obj, count);

    // when
    String strRepresentation = r.toString();

    // then
    assertThat(strRepresentation).contains(obj.toString()).contains("" + count);
  }

  @Test
  public void shouldReturnTheObject() {
    // given
    RankableObjectWithFields r = new RankableObjectWithFields(ANY_OBJECT, ANY_COUNT, ANY_FIELD);

    // when
    Object obj = r.getObject();

    // then
    assertThat(obj).isEqualTo(ANY_OBJECT);
  }

  @Test
  public void shouldReturnTheCount() {
    // given
    RankableObjectWithFields r = new RankableObjectWithFields(ANY_OBJECT, ANY_COUNT, ANY_FIELD);

    // when
    long count = r.getCount();

    // then
    assertThat(count).isEqualTo(ANY_COUNT);
  }

  @DataProvider
  public Object[][] fieldsData() {
    return new Object[][]{ { ANY_OBJECT, ANY_COUNT, new Object[]{ ANY_FIELD } },
        { "quux", 42L, new Object[]{ "one", "two", "three" } } };
  }

  @Test(dataProvider = "fieldsData")
  public void shouldReturnTheFields(Object obj, long count, Object[] fields) {
    // given
    RankableObjectWithFields r = new RankableObjectWithFields(obj, count, fields);

    // when
    List<Object> actualFields = r.getFields();

    // then
    assertThat(actualFields).isEqualTo(Lists.newArrayList(fields));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void fieldsShouldBeImmutable() {
    // given
    RankableObjectWithFields r = new RankableObjectWithFields(ANY_OBJECT, ANY_COUNT, ANY_FIELD);

    // when
    List<Object> fields = r.getFields();
    // try to modify the list, which should fail
    fields.remove(0);

    // then (exception)
  }

  @Test
  public void shouldCreateRankableObjectFromTuple() {
    // given
    Tuple tuple = mock(Tuple.class);
    List<Object> tupleValues = Lists.newArrayList(ANY_OBJECT, ANY_COUNT, ANY_FIELD);
    when(tuple.getValues()).thenReturn(tupleValues);

    // when
    RankableObjectWithFields r = RankableObjectWithFields.from(tuple);

    // then
    assertThat(r.getObject()).isEqualTo(ANY_OBJECT);
    assertThat(r.getCount()).isEqualTo(ANY_COUNT);
    List<Object> fields = new ArrayList<Object>();
    fields.add(ANY_FIELD);
    assertThat(r.getFields()).isEqualTo(fields);

  }

  @DataProvider
  public Object[][] copyData() {
    return new Object[][]{ { new RankableObjectWithFields("foo", 0) }, { new RankableObjectWithFields("foo", 3,
        "someOtherField") }, { new RankableObjectWithFields("foo", 0, "someField") } };
  }

  // TODO: What would be a good test to ensure that RankableObjectWithFields is at least somewhat defensively copied?
  //       The contract of Rankable#copy() returns a Rankable value, not a RankableObjectWithFields.
  @Test(dataProvider = "copyData")
  public void copyShouldReturnCopy(RankableObjectWithFields original) {
    // given

    // when
    Rankable copy = original.copy();

    // then
    assertThat(copy.getObject()).isEqualTo(original.getObject());
    assertThat(copy.getCount()).isEqualTo(original.getCount());
  }

}
