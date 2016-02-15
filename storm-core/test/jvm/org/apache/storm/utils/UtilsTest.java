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

package org.apache.storm.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.junit.Test;
import org.junit.Assert;

import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.apache.storm.Config;
import org.apache.thrift.transport.TTransportException;

import static org.mockito.Mockito.*;

public class UtilsTest{
    @Test
    public void newCuratorUsesExponentialBackoffTest() throws InterruptedException{
        final int expectedInterval = 2400;
        final int expectedRetries = 10;
        final int expectedCeiling = 3000;

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, expectedInterval); 
        config.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, expectedRetries); 
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, expectedCeiling); 

        CuratorFramework curator = Utils.newCurator(config, Arrays.asList("bogus_server"), 42 /*port*/, "");
        StormBoundedExponentialBackoffRetry policy = 
            (StormBoundedExponentialBackoffRetry) curator.getZookeeperClient().getRetryPolicy();
        Assert.assertEquals(policy.getBaseSleepTimeMs(), expectedInterval);
        Assert.assertEquals(policy.getN(), expectedRetries);
        Assert.assertEquals(policy.getSleepTimeMs(10, 0), expectedCeiling);
    }

    @Test(expected = RuntimeException.class)
    public void getConfiguredClientThrowsRuntimeExceptionOnBadArgsTest () throws RuntimeException, TTransportException {
        Map config = ConfigUtils.readStormConfig();
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
        new NimbusClient(config, "", 65535);
    }

    private Map mockMap(String key, String value){
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    private Map topologyMockMap(String value){
        return mockMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, value);
    }

    private Map serverMockMap(String value){
        return mockMap(Config.STORM_ZOOKEEPER_AUTH_SCHEME, value);
    }

    private Map emptyMockMap(){
        return new HashMap<String, Object>();
    }

    /* isZkAuthenticationConfiguredTopology */
    @Test
    public void isZkAuthenticationConfiguredTopologyReturnsFalseOnNullConfigTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(null));
    }

    @Test
    public void isZkAuthenticationConfiguredTopologyReturnsFalseOnSchemeKeyMissingTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(emptyMockMap()));
    }

    @Test
    public void isZkAuthenticationConfiguredTopologyReturnsFalseOnSchemeValueNullTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(topologyMockMap(null)));
    }

    @Test
    public void isZkAuthenticationConfiguredTopologyReturnsTrueWhenSchemeSetToStringTest(){
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredTopology(topologyMockMap("foobar")));
    }

    /* isZkAuthenticationConfiguredStormServer */
    @Test
    public void isZkAuthenticationConfiguredStormReturnsFalseOnNullConfigTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(null));
    }

    @Test
    public void isZkAuthenticationConfiguredStormReturnsFalseOnSchemeKeyMissingTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
    }

    @Test
    public void isZkAuthenticationConfiguredStormReturnsFalseOnSchemeValueNullTest(){
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(serverMockMap(null)));
    }

    @Test
    public void isZkAuthenticationConfiguredStormReturnsTrueWhenSchemeSetToStringTest(){
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(serverMockMap("foobar")));
    }

    @Test
    public void isZkAuthenticationConfiguredStormReturnsTrueWhenAuthLoginConfigIsSetTest(){
        String key = "java.security.auth.login.config";
        String oldValue = System.getProperty(key);
        try {
            System.setProperty("java.security.auth.login.config", "anything");
            Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
        } catch (Exception ignore) {
        } finally {
            // reset property
            if (oldValue == null){
                System.clearProperty(key);
            } else {
                System.setProperty(key, oldValue);
            }
        }
    }

    private CuratorFrameworkFactory.Builder setupBuilder(boolean withExhibitor){
        return setupBuilder(withExhibitor, false /*without Auth*/);
    }

    private CuratorFrameworkFactory.Builder setupBuilder(boolean withExhibitor, boolean withAuth){
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        Map<String, Object> conf = new HashMap<String, Object>();
        if (withExhibitor){
            conf.put(Config.STORM_EXHIBITOR_SERVERS,"foo");
            conf.put(Config.STORM_EXHIBITOR_PORT, 0);
            conf.put(Config.STORM_EXHIBITOR_URIPATH, "/exhibitor");
            conf.put(Config.STORM_EXHIBITOR_POLL, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_INTERVAL, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_INTERVAL_CEILING, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_TIMES, 0);
        }
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 0);
        String zkStr = new String("zk_connection_string");
        ZookeeperAuthInfo auth = null;
        if (withAuth){
            auth = new ZookeeperAuthInfo("scheme", "abc".getBytes());
        }
        Utils.testSetupBuilder(builder, zkStr, conf, auth);
        return builder;
    }

    @Test
    public void ifExhibitorServersProvidedBuilderUsesTheExhibitorEnsembleProviderTest(){
        CuratorFrameworkFactory.Builder builder = setupBuilder(true /*with exhibitor*/);
        Assert.assertEquals(builder.getEnsembleProvider().getConnectionString(), "");
        Assert.assertEquals(builder.getEnsembleProvider().getClass(), ExhibitorEnsembleProvider.class);
    }

    @Test
    public void ifExhibitorServersAreEmptyBuilderUsesAFixedEnsembleProviderTest(){
        CuratorFrameworkFactory.Builder builder = setupBuilder(false /*without exhibitor*/);
        Assert.assertEquals(builder.getEnsembleProvider().getConnectionString(), "zk_connection_string");
        Assert.assertEquals(builder.getEnsembleProvider().getClass(), FixedEnsembleProvider.class);
    }

    @Test
    public void ifAuthSchemeAndPayloadAreDefinedBuilderUsesAuthTest(){
        CuratorFrameworkFactory.Builder builder = setupBuilder(false /*without exhibitor*/, true /*with auth*/);
        List<AuthInfo> authInfos = builder.getAuthInfos();
        AuthInfo authInfo = authInfos.get(0);
        Assert.assertEquals(authInfo.getScheme(), "scheme"); 
        Assert.assertArrayEquals(authInfo.getAuth(), "abc".getBytes());
    }

    @Test
    public void parseJvmHeapMemByChildOpts1024KIs1Test(){
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts("Xmx1024K", 0.0).doubleValue(), 1.0, 0); 
    }

    @Test
    public void parseJvmHeapMemByChildOpts100MIs100Test(){
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts("Xmx100M", 0.0).doubleValue(), 100.0, 0); 
    }

    @Test
    public void parseJvmHeapMemByChildOpts1GIs1024Test(){
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts("Xmx1G", 0.0).doubleValue(), 1024.0, 0); 
    }

    @Test
    public void parseJvmHeapMemByChildOptsReturnsDefaultIfMatchNotFoundTest(){
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts("Xmx1T", 123.0).doubleValue(), 123.0, 0); 
    }

    @Test
    public void parseJvmHeapMemByChildOptsReturnsDefaultIfInputIsNullTest(){
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(null, 123.0).doubleValue(), 123.0, 0); 
    }
}
