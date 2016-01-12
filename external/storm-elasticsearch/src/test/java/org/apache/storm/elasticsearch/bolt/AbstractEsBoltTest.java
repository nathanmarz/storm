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
package org.apache.storm.elasticsearch.bolt;

import com.google.common.testing.NullPointerTester;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractEsBoltTest<Bolt extends AbstractEsBolt> {

    protected static Config config = new Config();

    @Mock
    protected OutputCollector outputCollector;

    protected Bolt bolt;

    @Before
    public void createBolt() throws Exception {
        bolt = createBolt(esConfig());
        bolt.prepare(config, null, outputCollector);
    }

    protected abstract Bolt createBolt(EsConfig esConfig);

    protected EsConfig esConfig() {
        return new EsConfig("test-cluster", new String[] {"127.0.0.1:9300"});
    }

    @After
    public void cleanupBolt() throws Exception {
        bolt.cleanup();
    }

    @Test
    public void constructorsThrowOnNull() throws Exception {
        new NullPointerTester().setDefault(EsConfig.class, esConfig()).testAllPublicConstructors(getBoltClass());
    }

    protected abstract Class<Bolt> getBoltClass();
}
