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

package org.apache.storm.command;

import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SetLogLevelTest {

    @Test
    public void testUpdateLogLevelParser() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        LogLevel logLevel = ((Map<String, LogLevel>) logLevelsParser.parse("com.foo.one=warn")).get("com.foo.one");
        Assert.assertEquals(0, logLevel.get_reset_log_level_timeout_secs());
        Assert.assertEquals("WARN", logLevel.get_reset_log_level());

        logLevel = ((Map<String, LogLevel>) logLevelsParser.parse("com.foo.two=DEBUG:10")).get("com.foo.two");
        Assert.assertEquals(10, logLevel.get_reset_log_level_timeout_secs());
        Assert.assertEquals("DEBUG", logLevel.get_reset_log_level());
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidTimeout() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        logLevelsParser.parse("com.foo.bar=warn:NaN");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLogLevel() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        logLevelsParser.parse("com.foo.bar=CRITICAL");
    }

}
