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
package storm.kafka;

import backtype.storm.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Date: 16/05/2013
 * Time: 20:35
 */
public class DynamicBrokersReaderTest {
    private DynamicBrokersReader dynamicBrokersReader, wildCardBrokerReader;
    private String masterPath = "/brokers";
    private String topic = "testing1";
    private String secondTopic = "testing2";
    private String thirdTopic = "testing3";

    private CuratorFramework zookeeper;
    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        dynamicBrokersReader = new DynamicBrokersReader(conf, connectionString, masterPath, topic);

        Map conf2 = new HashMap();
        conf2.putAll(conf);
        conf2.put("kafka.topic.wildcard.match",true);

        wildCardBrokerReader = new DynamicBrokersReader(conf2, connectionString, masterPath, "^test.*$");
        zookeeper.start();
    }

    @After
    public void tearDown() throws Exception {
        dynamicBrokersReader.close();
        zookeeper.close();
        server.close();
    }

    private void addPartition(int id, String host, int port, String topic) throws Exception {
        writePartitionId(id, topic);
        writeLeader(id, 0, topic);
        writeLeaderDetails(0, host, port);
    }

    private void addPartition(int id, int leader, String host, int port, String topic) throws Exception {
        writePartitionId(id, topic);
        writeLeader(id, leader, topic);
        writeLeaderDetails(leader, host, port);
    }

    private void writePartitionId(int id, String topic) throws Exception {
        String path = dynamicBrokersReader.partitionPath(topic);
        writeDataToPath(path, ("" + id));
    }

    private void writeDataToPath(String path, String data) throws Exception {
        ZKPaths.mkdirs(zookeeper.getZookeeperClient().getZooKeeper(), path);
        zookeeper.setData().forPath(path, data.getBytes());
    }

    private void writeLeader(int id, int leaderId, String topic) throws Exception {
        String path = dynamicBrokersReader.partitionPath(topic) + "/" + id + "/state";
        String value = " { \"controller_epoch\":4, \"isr\":[ 1, 0 ], \"leader\":" + leaderId + ", \"leader_epoch\":1, \"version\":1 }";
        writeDataToPath(path, value);
    }

    private void writeLeaderDetails(int leaderId, String host, int port) throws Exception {
        String path = dynamicBrokersReader.brokerPath() + "/" + leaderId;
        String value = "{ \"host\":\"" + host + "\", \"jmx_port\":9999, \"port\":" + port + ", \"version\":1 }";
        writeDataToPath(path, value);
    }


    private GlobalPartitionInformation getByTopic(List<GlobalPartitionInformation> partitions, String topic){
        for(GlobalPartitionInformation partitionInformation : partitions) {
            if (partitionInformation.topic.equals(topic)) return partitionInformation;
        }
        return null;
    }

    @Test
    public void testGetBrokerInfo() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port, topic);
        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();

        GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(1, brokerInfo.getOrderedPartitions().size());
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);
    }

    @Test
    public void testGetBrokerInfoWildcardMatch() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port, topic);
        addPartition(partition, host, port, secondTopic);

        List<GlobalPartitionInformation> partitions = wildCardBrokerReader.getBrokerInfo();

        GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(1, brokerInfo.getOrderedPartitions().size());
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        brokerInfo = getByTopic(partitions, secondTopic);
        assertNotNull(brokerInfo);
        assertEquals(1, brokerInfo.getOrderedPartitions().size());
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        addPartition(partition, host, port, thirdTopic);
        //Discover newly added topic
        partitions = wildCardBrokerReader.getBrokerInfo();
        assertNotNull(getByTopic(partitions, topic));
        assertNotNull(getByTopic(partitions, secondTopic));
        assertNotNull(getByTopic(partitions, secondTopic));
    }


    @Test
    public void testMultiplePartitionsOnDifferentHosts() throws Exception {
        String host = "localhost";
        int port = 9092;
        int secondPort = 9093;
        int partition = 0;
        int secondPartition = partition + 1;
        addPartition(partition, 0, host, port, topic);
        addPartition(secondPartition, 1, host, secondPort, topic);

        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();

        GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(2, brokerInfo.getOrderedPartitions().size());

        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        assertEquals(secondPort, brokerInfo.getBrokerFor(secondPartition).port);
        assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
    }


    @Test
    public void testMultiplePartitionsOnSameHost() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        int secondPartition = partition + 1;
        addPartition(partition, 0, host, port, topic);
        addPartition(secondPartition, 0, host, port, topic);

        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();

        GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(2, brokerInfo.getOrderedPartitions().size());

        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        assertEquals(port, brokerInfo.getBrokerFor(secondPartition).port);
        assertEquals(host, brokerInfo.getBrokerFor(secondPartition).host);
    }

    @Test
    public void testSwitchHostForPartition() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port, topic);
        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();

        GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        String newHost = host + "switch";
        int newPort = port + 1;
        addPartition(partition, newHost, newPort, topic);
        partitions = dynamicBrokersReader.getBrokerInfo();

        brokerInfo = getByTopic(partitions, topic);
        assertNotNull(brokerInfo);
        assertEquals(newPort, brokerInfo.getBrokerFor(partition).port);
        assertEquals(newHost, brokerInfo.getBrokerFor(partition).host);
    }

    @Test(expected = NullPointerException.class)
    public void testErrorLogsWhenConfigIsMissing() throws Exception {
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
//        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);

        DynamicBrokersReader dynamicBrokersReader1 = new DynamicBrokersReader(conf, connectionString, masterPath, topic);

    }
}
