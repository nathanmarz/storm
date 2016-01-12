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

package org.apache.storm;

import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.ConfigValidation.*;
import org.apache.storm.validation.ConfigValidationAnnotations.*;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestConfigValidate {

    private static final Logger LOG = LoggerFactory.getLogger(TestConfigValidate.class);

    @Test
    public void validPacemakerAuthTest() throws InstantiationException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.PACEMAKER_AUTH_METHOD, "NONE");
        ConfigValidation.validateFields(conf);
        conf.put(Config.PACEMAKER_AUTH_METHOD, "DIGEST");
        ConfigValidation.validateFields(conf);
        conf.put(Config.PACEMAKER_AUTH_METHOD, "KERBEROS");
        ConfigValidation.validateFields(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPacemakerAuthTest() throws InstantiationException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.PACEMAKER_AUTH_METHOD, "invalid");
        ConfigValidation.validateFields(conf);
    }
    
    @Test
    public void validConfigTest() throws InstantiationException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {


        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG, 5);
        conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 500);
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, true);

        ConfigValidation.validateFields(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidConfigTest() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG, 5);
        conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 500);
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, "invalid");

        ConfigValidation.validateFields(conf);
    }

    @Test
    public void defaultYamlTest() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
    }

    @Test
    public void testTopologyWorkersIsInteger() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_WORKERS, 42);
        ConfigValidation.validateFields(conf);

        conf.put(Config.TOPOLOGY_WORKERS, 3.14159);
        try {
            ConfigValidation.validateFields(conf);
            Assert.fail("Expected Exception not Thrown");
        } catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void testTopologyStatsSampleRateIsFloat() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.5);
        ConfigValidation.validateFields(conf);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 10);
        ConfigValidation.validateFields(conf);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, Double.MAX_VALUE);
        ConfigValidation.validateFields(conf);
    }

    @Test
    public void testIsolationSchedulerMachinesIsMap() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        Map<String, Integer> isolationMap = new HashMap<String, Integer>();
        conf.put(Config.ISOLATION_SCHEDULER_MACHINES, isolationMap);
        ConfigValidation.validateFields(conf);

        isolationMap.put("host0", 1);
        isolationMap.put("host1", 2);

        conf.put(Config.ISOLATION_SCHEDULER_MACHINES, isolationMap);
        ConfigValidation.validateFields(conf);

        conf.put(Config.ISOLATION_SCHEDULER_MACHINES, 42);
        try {
            ConfigValidation.validateFields(conf);
            Assert.fail("Expected Exception not Thrown");
        } catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void testWorkerChildoptsIsStringOrStringList() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();

        passCases.add(null);
        passCases.add("some string");
        String[] stuff = {"some", "string", "list"};
        passCases.add(Arrays.asList(stuff));

        failCases.add(42);
        Integer[] wrongStuff = {1, 2, 3};
        failCases.add(Arrays.asList(wrongStuff));

        //worker.childopts validates
        for (Object value : passCases) {
            conf.put(Config.WORKER_CHILDOPTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            try {
                conf.put(Config.WORKER_CHILDOPTS, value);
                ConfigValidation.validateFields(conf);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }

        //topology.worker.childopts validates
        conf = new HashMap<String, Object>();
        for (Object value : passCases) {
            conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            try {
                conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, value);
                ConfigValidation.validateFields(conf);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testSupervisorSlotsPorts() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Map<String, Object> conf = new HashMap<String, Object>();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();

        Integer[] test1 = {1233, 1234, 1235};
        Integer[] test2 = {1233};
        passCases.add(Arrays.asList(test1));
        passCases.add(Arrays.asList(test2));

        String[] test3 = {"1233", "1234", "1235"};
        //duplicate case
        Integer[] test4 = {1233, 1233, 1235};
        failCases.add(test3);
        failCases.add(test4);
        failCases.add(null);
        failCases.add("1234");
        failCases.add(1234);

        for (Object value : passCases) {
            conf.put(Config.SUPERVISOR_SLOTS_PORTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            try {
                conf.put(Config.SUPERVISOR_SLOTS_PORTS, value);
                ConfigValidation.validateFields(conf);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testValidity() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put("q", "asasdasd");
        conf.put("aaa", new Integer("123"));
        conf.put("bbb", new Long("456"));
        List<Object> testList = new ArrayList<Object>();
        testList.add(1);
        testList.add(2);
        testList.add(new Integer("3"));
        testList.add(new Long("4"));
        conf.put("eee", testList);
        Utils.isValidConf(conf);
    }

    @Test
    public void testPowerOf2Validator() {
        PowerOf2Validator validator = new PowerOf2Validator();

        Object[] failCases = {42.42, 42, -33, 23423423423.0, -32, -1, -0.00001, 0, -0, "Forty-two"};
        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }

        Object[] passCases = {64, 4294967296.0, 1, null};
        for (Object value : passCases) {
            validator.validateField("test", value);
        }
    }

    @Test
    public void testPositiveNumberValidator() {
        PositiveNumberValidator validator = new PositiveNumberValidator();

        Object[] passCases = {null, 1.0, 0.01, 1, 2147483647, 42};

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCases = {-1.0, -1, -0.01, 0.0, 0, "43", "string"};

        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }

        Object[] passCasesIncludeZero = {null, 1.0, 0.01, 0, 2147483647, 0.0};

        for (Object value : passCasesIncludeZero) {
            validator.validateField("test", true, value);
        }

        Object[] failCasesIncludeZero = {-1.0, -1, -0.01, "43", "string"};

        for (Object value : failCasesIncludeZero) {
            try {
                validator.validateField("test", true, value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testIntegerValidator() {
        IntegerValidator validator = new IntegerValidator();

        Object[] passCases = {null, 1000, Integer.MAX_VALUE};

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCases = {1.34, new Long(Integer.MAX_VALUE) + 1};

        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void NoDuplicateInListValidator() {
        NoDuplicateInListValidator validator = new NoDuplicateInListValidator();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();

        Object[] passCase1 = {1000, 0, -1000};
        Object[] passCase2 = {"one", "two", "three"};
        Object[] passCase3 = {false, true};
        Object[] passCase4 = {false, true, 1000, 0, -1000, "one", "two", "three"};
        Object[] passCase5 = {1000.0, 0.0, -1000.0};
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));
        passCases.add(Arrays.asList(passCase3));
        passCases.add(Arrays.asList(passCase4));
        passCases.add(Arrays.asList(passCase5));
        passCases.add(null);

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCase1 = {1000, 0, 1000};
        Object[] failCase2 = {"one", "one", "two"};
        Object[] failCase3 = {5.0, 5.0, 6};
        failCases.add(Arrays.asList(failCase1));
        failCases.add(Arrays.asList(failCase2));
        failCases.add(Arrays.asList(failCase3));
        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testListEntryTypeValidator() {
        Collection<Object> testCases1 = new LinkedList<Object>();
        Collection<Object> testCases2 = new LinkedList<Object>();
        Collection<Object> testCases3 = new LinkedList<Object>();

        Object[] testCase1 = {"one", "two", "three"};
        ;
        Object[] testCase2 = {"three"};
        testCases1.add(Arrays.asList(testCase1));
        testCases1.add(Arrays.asList(testCase2));

        for (Object value : testCases1) {
            ListEntryTypeValidator.validateField("test", String.class, value);
        }

        for (Object value : testCases1) {
            try {
                ListEntryTypeValidator.validateField("test", Number.class, value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }

        Object[] testCase3 = {1000, 0, 1000};
        Object[] testCase4 = {5};
        Object[] testCase5 = {5.0, 5.0, 6};
        testCases2.add(Arrays.asList(testCase3));
        testCases2.add(Arrays.asList(testCase4));
        testCases2.add(Arrays.asList(testCase5));
        for (Object value : testCases2) {
            try {
                ListEntryTypeValidator.validateField("test", String.class, value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
        for (Object value : testCases2) {
            ListEntryTypeValidator.validateField("test", Number.class, value);
        }

        Object[] testCase6 = {1000, 0, 1000, "5"};
        Object[] testCase7 = {"4", "5", 5};
        testCases3.add(Arrays.asList(testCase6));
        testCases3.add(Arrays.asList(testCase7));
        for (Object value : testCases3) {
            try {
                ListEntryTypeValidator.validateField("test", String.class, value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
        for (Object value : testCases1) {
            try {
                ListEntryTypeValidator.validateField("test", Number.class, value);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testMapEntryTypeAnnotation() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();
        Map<Object, Object> passCase1 = new HashMap<Object, Object>();
        passCase1.put("aaa", 5);
        passCase1.put("bbb", 6);
        passCase1.put("ccc", 7);
        passCases.add(passCase1);
        passCases.add(null);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        Map<Object, Object> failCase1 = new HashMap<Object, Object>();
        failCase1.put("aaa", 5);
        failCase1.put(5, 6);
        failCase1.put("ccc", 7);
        Map<Object, Object> failCase2 = new HashMap<Object, Object>();
        failCase2.put("aaa", "str");
        failCase2.put("bbb", 6);
        failCase2.put("ccc", 7);

        failCases.add(failCase1);
        failCases.add(failCase2);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testMapEntryCustomAnnotation() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();
        Map<Object, Object> passCase1 = new HashMap<Object, Object>();
        passCase1.put("aaa", 5);
        passCase1.put("bbb", 100);
        passCase1.put("ccc", Integer.MAX_VALUE);
        passCases.add(passCase1);
        passCases.add(null);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_2, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        Map<Object, Object> failCase1 = new HashMap<Object, Object>();
        failCase1.put("aaa", 5);
        failCase1.put(5, 6);
        failCase1.put("ccc", 7);
        Map<Object, Object> failCase2 = new HashMap<Object, Object>();
        failCase2.put("aaa", "str");
        failCase2.put("bbb", 6);
        failCase2.put("ccc", 7);
        Map<Object, Object> failCase3 = new HashMap<Object, Object>();
        failCase3.put("aaa", -1);
        failCase3.put("bbb", 6);
        failCase3.put("ccc", 7);
        Map<Object, Object> failCase4 = new HashMap<Object, Object>();
        failCase4.put("aaa", 1);
        failCase4.put("bbb", 6);
        failCase4.put("ccc", 7.4);

        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add(failCase3);
        failCases.add(failCase4);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_2, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testListEntryTypeAnnotation() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();
        Object[] passCase1 = {1, 5.0, -0.01, 0, Integer.MAX_VALUE, Double.MIN_VALUE};
        Object[] passCase2 = {1};
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_3, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        Object[] failCase1 = {1, 5.0, -0.01, 0, "aaa"};
        Object[] failCase2 = {"aaa"};
        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add(1);
        failCases.add("b");
        failCases.add(null);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_3, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void testListEntryCustomAnnotation() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();
        Object[] passCase1 = {1, 5.0, 0.01, Double.MAX_VALUE};
        Object[] passCase2 = {1};
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_4, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        Object[] failCase1 = {1, 5.0, -0.01, 3.0};
        Object[] failCase2 = {1, 5.0, -0.01, 1};
        Object[] failCase3 = {"aaa", "bbb", "aaa"};
        Object[] failCase4 = {1, 5.0, null, 1};
        Object[] failCase5 = {1, 5.0, 0, 1};

        failCases.add(Arrays.asList(failCase1));
        failCases.add(Arrays.asList(failCase2));
        failCases.add(Arrays.asList(failCase3));
        failCases.add(Arrays.asList(failCase4));
        failCases.add(Arrays.asList(failCase5));
        failCases.add(1);
        failCases.add("b");
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_4, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void TestAcceptedStrings() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        String[] passCases = {"aaa", "bbb", "ccc"};

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_5, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        String[] failCases = {"aa", "bb", "cc", "abc", "a", "b", "c", ""};
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_5, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void TestImpersonationAclUserEntryValidator() throws InvocationTargetException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();

        Map<String, Map<String, List<String>>> passCase1 = new HashMap<String, Map<String, List<String>>>();
        Map<String, List<String>> passCase1_hostsAndGroups = new HashMap<String, List<String>>();
        String[] hosts = {"host.1", "host.2", "host.3"};
        passCase1_hostsAndGroups.put("hosts", Arrays.asList(hosts));
        String[] groups = {"group.1", "group.2", "group.3"};
        passCase1_hostsAndGroups.put("groups", Arrays.asList(groups));
        passCase1.put("jerry", passCase1_hostsAndGroups);
        passCases.add(passCase1);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_6, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }

        Map<String, Map<String, List<String>>> failCase1 = new HashMap<String, Map<String, List<String>>>();
        Map<String, List<String>> failCase1_hostsAndGroups = new HashMap<String, List<String>>();
        String[] failhosts = {"host.1", "host.2", "host.3"};
        failCase1_hostsAndGroups.put("hosts", Arrays.asList(hosts));
        failCase1.put("jerry", failCase1_hostsAndGroups);


        Map<String, Map<String, List<String>>> failCase2 = new HashMap<String, Map<String, List<String>>>();
        Map<String, List<String>> failCase2_hostsAndGroups = new HashMap<String, List<String>>();
        String[] failgroups = {"group.1", "group.2", "group.3"};
        failCase2_hostsAndGroups.put("groups", Arrays.asList(groups));
        failCase2.put("jerry", failCase2_hostsAndGroups);

        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add("stuff");
        failCases.add(5);

        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_6, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void TestResourceAwareSchedulerUserPool() {
        TestConfig config = new TestConfig();
        Collection<Object> failCases = new LinkedList<Object>();

        Map<String, Map<String, Integer>> passCase1 = new HashMap<String, Map<String, Integer>>();
        passCase1.put("jerry", new HashMap<String, Integer>());
        passCase1.put("bobby", new HashMap<String, Integer>());
        passCase1.put("derek", new HashMap<String, Integer>());

        passCase1.get("jerry").put("cpu", 10000);
        passCase1.get("jerry").put("memory", 20148);
        passCase1.get("bobby").put("cpu", 20000);
        passCase1.get("bobby").put("memory", 40148);
        passCase1.get("derek").put("cpu", 30000);
        passCase1.get("derek").put("memory", 60148);

        config.put(TestConfig.TEST_MAP_CONFIG_7, (Object) passCase1);
        ConfigValidation.validateFields(config, TestConfig.class);

        Map<String, Map<String, Integer>> failCase1 = new HashMap<String, Map<String, Integer>>();
        failCase1.put("jerry", new HashMap<String, Integer>());
        failCase1.put("bobby", new HashMap<String, Integer>());
        failCase1.put("derek", new HashMap<String, Integer>());

        failCase1.get("jerry").put("cpu", 10000);
        failCase1.get("jerry").put("memory", 20148);
        failCase1.get("bobby").put("cpu", 20000);
        failCase1.get("bobby").put("memory", 40148);
        //this will fail the test since user derek does not have an entry for memory
        failCase1.get("derek").put("cpu", 30000);

        Map<String, Map<String, Integer>> failCase2 = new HashMap<String, Map<String, Integer>>();
        //this will fail since jerry doesn't have either cpu or memory entries
        failCase2.put("jerry", new HashMap<String, Integer>());
        failCase2.put("bobby", new HashMap<String, Integer>());
        failCase2.put("derek", new HashMap<String, Integer>());
        failCase2.get("bobby").put("cpu", 20000);
        failCase2.get("bobby").put("memory", 40148);
        failCase2.get("derek").put("cpu", 30000);
        failCase2.get("derek").put("memory", 60148);

        failCases.add(failCase1);
        failCases.add(failCase2);

        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_7, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    @Test
    public void TestImplementsClassValidator() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<Object>();
        Collection<Object> failCases = new LinkedList<Object>();

        passCases.add("org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_8, value);
            ConfigValidation.validateFields(config, TestConfig.class);
        }
        //will fail since org.apache.storm.nimbus.NimbusInfo doesn't implement or extend org.apache.storm.networktopography.DNSToSwitchMapping
        failCases.add("org.apache.storm.nimbus.NimbusInfo");
        failCases.add(null);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_8, value);
                ConfigValidation.validateFields(config, TestConfig.class);
                Assert.fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException Ex) {
            }
        }
    }

    public class TestConfig extends HashMap<String, Object> {
        @isMapEntryType(keyType = String.class, valueType = Integer.class)
        public static final String TEST_MAP_CONFIG = "test.map.config";

        @isMapEntryCustom(
                keyValidatorClasses = {StringValidator.class},
                valueValidatorClasses = {PositiveNumberValidator.class, IntegerValidator.class})
        public static final String TEST_MAP_CONFIG_2 = "test.map.config.2";

        @isListEntryType(type = Number.class)
        @NotNull
        public static final String TEST_MAP_CONFIG_3 = "test.map.config.3";

        @isListEntryCustom(
                entryValidatorClasses = {PositiveNumberValidator.class, NotNullValidator.class})
        @isNoDuplicateInList
        public static final String TEST_MAP_CONFIG_4 = "test.map.config.4";

        @isString(acceptedValues = {"aaa", "bbb", "ccc"})
        public static final String TEST_MAP_CONFIG_5 = "test.map.config.5";

        @isMapEntryCustom(keyValidatorClasses = {StringValidator.class}, valueValidatorClasses = {ImpersonationAclUserEntryValidator.class})
        public static final String TEST_MAP_CONFIG_6 = "test.map.config.6";

        @isMapEntryCustom(keyValidatorClasses = {StringValidator.class}, valueValidatorClasses = {UserResourcePoolEntryValidator.class})
        public static final String TEST_MAP_CONFIG_7 = "test.map.config.7";

        @isImplementationOfClass(implementsClass = org.apache.storm.networktopography.DNSToSwitchMapping.class)
        @NotNull
        public static final String TEST_MAP_CONFIG_8 = "test.map.config.8";
    }
}
