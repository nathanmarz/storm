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
package org.apache.storm.kafka;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Date: 12/01/2014
 * Time: 18:09
 */
public class KafkaErrorTest {

    @Test
    public void getError() {
        assertThat(KafkaError.getError(0), is(equalTo(KafkaError.NO_ERROR)));
    }

    @Test
    public void offsetMetaDataTooLarge() {
        assertThat(KafkaError.getError(12), is(equalTo(KafkaError.OFFSET_METADATA_TOO_LARGE)));
    }

    @Test
    public void unknownNegative() {
        assertThat(KafkaError.getError(-1), is(equalTo(KafkaError.UNKNOWN)));
    }

    @Test
    public void unknownPositive() {
        assertThat(KafkaError.getError(75), is(equalTo(KafkaError.UNKNOWN)));
    }

    @Test
    public void unknown() {
        assertThat(KafkaError.getError(13), is(equalTo(KafkaError.UNKNOWN)));
    }
}
