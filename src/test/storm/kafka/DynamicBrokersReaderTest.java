package storm.kafka;

import backtype.storm.Config;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Date: 16/05/2013
 * Time: 20:35
 */
public class DynamicBrokersReaderTest {
    private DynamicBrokersReader dynamicBrokersReader;
    private String masterPath = "/brokers";
    private String topic = "testing";
    private CuratorFramework zookeeper;
    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        dynamicBrokersReader = new DynamicBrokersReader(conf, connectionString, masterPath, topic);
        zookeeper.start();
    }

    @After
    public void tearDown() throws Exception {
        dynamicBrokersReader.close();
        zookeeper.close();
        server.close();
    }

    private void addPartition(int id, String host, int port) throws Exception {
        writePartitionId(id);
        writeLeader(id, 0);
        writeLeaderDetails(0, host, port);
    }

    private void addPartition(int id, int leader, String host, int port) throws Exception {
        writePartitionId(id);
        writeLeader(id, leader);
        writeLeaderDetails(leader, host, port);
    }

    private void writePartitionId(int id) throws Exception {
        String path = dynamicBrokersReader.partitionPath();
        writeDataToPath(path, ("" + id));
    }

    private void writeDataToPath(String path, String data) throws Exception {
        ZKPaths.mkdirs(zookeeper.getZookeeperClient().getZooKeeper(), path);
        zookeeper.setData().forPath(path, data.getBytes());
    }

    private void writeLeader(int id, int leaderId) throws Exception {
        String path = dynamicBrokersReader.partitionPath() + "/" + id + "/state";
        String value = " { \"controller_epoch\":4, \"isr\":[ 1, 0 ], \"leader\":" + leaderId + ", \"leader_epoch\":1, \"version\":1 }";
        writeDataToPath(path, value);
    }

    private void writeLeaderDetails(int leaderId, String host, int port) throws Exception {
        String path = dynamicBrokersReader.brokerPath() + "/" + leaderId;
        String value = "{ \"host\":\"" + host + "\", \"jmx_port\":9999, \"port\":" + port + ", \"version\":1 }";
        writeDataToPath(path, value);
    }

    @Test
    public void testGetBrokerInfo() throws Exception {
        String host = "localhost";
        int port = 9092;
        int partition = 0;
        addPartition(partition, host, port);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(1, brokerInfo.getOrderedPartitions().size());
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);
    }


    @Test
    public void testMultiplePartitionsOnDifferentHosts() throws Exception {
        String host = "localhost";
        int port = 9092;
        int secondPort = 9093;
        int partition = 0;
        int secondPartition = partition + 1;
        addPartition(partition, 0, host, port);
        addPartition(secondPartition, 1, host, secondPort);

        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
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
        addPartition(partition, 0, host, port);
        addPartition(secondPartition, 0, host, port);

        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
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
        addPartition(partition, host, port);
        GlobalPartitionInformation brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(port, brokerInfo.getBrokerFor(partition).port);
        assertEquals(host, brokerInfo.getBrokerFor(partition).host);

        String newHost = host + "switch";
        int newPort = port + 1;
        addPartition(partition, newHost, newPort);
        brokerInfo = dynamicBrokersReader.getBrokerInfo();
        assertEquals(newPort, brokerInfo.getBrokerFor(partition).port);
        assertEquals(newHost, brokerInfo.getBrokerFor(partition).host);
    }
}
