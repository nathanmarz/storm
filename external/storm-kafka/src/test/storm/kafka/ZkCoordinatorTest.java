package storm.kafka;

import backtype.storm.Config;
import com.netflix.curator.test.TestingServer;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

public class ZkCoordinatorTest {


    @Mock
    private DynamicBrokersReader reader;

    @Mock
    private DynamicPartitionConnections dynamicPartitionConnections;

    private KafkaTestBroker broker = new KafkaTestBroker();
    private TestingServer server;
    private Map stormConf = new HashMap();
    private SpoutConfig spoutConfig;
    private ZkState state;
    private SimpleConsumer simpleConsumer;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = new TestingServer();
        String connectionString = server.getConnectString();
        ZkHosts hosts = new ZkHosts(connectionString);
        hosts.refreshFreqSecs = 1;
        spoutConfig = new SpoutConfig(hosts, "topic", "/test", "id");
        Map conf = buildZookeeperConfig(server);
        state = new ZkState(conf);
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
        when(dynamicPartitionConnections.register(any(Broker.class), anyInt())).thenReturn(simpleConsumer);
    }

    private Map buildZookeeperConfig(TestingServer server) {
        Map conf = new HashMap();
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, server.getPort());
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 20000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 3);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 30);
        return conf;
    }

    @After
    public void shutdown() throws Exception {
        simpleConsumer.close();
        broker.shutdown();
        server.close();
    }

    @Test
    public void testOnePartitionPerTask() throws Exception {
        int totalTasks = 64;
        int partitionsPerTask = 1;
        List<ZkCoordinator> coordinatorList = buildCoordinators(totalTasks / partitionsPerTask);
        when(reader.getBrokerInfo()).thenReturn(TestUtils.buildPartitionInfo(totalTasks));
        for (ZkCoordinator coordinator : coordinatorList) {
            List<PartitionManager> myManagedPartitions = coordinator.getMyManagedPartitions();
            assertEquals(partitionsPerTask, myManagedPartitions.size());
            assertEquals(coordinator._taskIndex, myManagedPartitions.get(0).getPartition().partition);
        }
    }


    @Test
    public void testPartitionsChange() throws Exception {
        final int totalTasks = 64;
        int partitionsPerTask = 2;
        List<ZkCoordinator> coordinatorList = buildCoordinators(totalTasks / partitionsPerTask);
        when(reader.getBrokerInfo()).thenReturn(TestUtils.buildPartitionInfo(totalTasks, 9092));
        List<List<PartitionManager>> partitionManagersBeforeRefresh = getPartitionManagers(coordinatorList);
        waitForRefresh();
        when(reader.getBrokerInfo()).thenReturn(TestUtils.buildPartitionInfo(totalTasks, 9093));
        List<List<PartitionManager>> partitionManagersAfterRefresh = getPartitionManagers(coordinatorList);
        assertEquals(partitionManagersAfterRefresh.size(), partitionManagersAfterRefresh.size());
        Iterator<List<PartitionManager>> iterator = partitionManagersAfterRefresh.iterator();
        for (List<PartitionManager> partitionManagersBefore : partitionManagersBeforeRefresh) {
            List<PartitionManager> partitionManagersAfter = iterator.next();
            assertPartitionsAreDifferent(partitionManagersBefore, partitionManagersAfter, partitionsPerTask);
        }
    }

    private void assertPartitionsAreDifferent(List<PartitionManager> partitionManagersBefore, List<PartitionManager> partitionManagersAfter, int partitionsPerTask) {
        assertEquals(partitionsPerTask, partitionManagersBefore.size());
        assertEquals(partitionManagersBefore.size(), partitionManagersAfter.size());
        for (int i = 0; i < partitionsPerTask; i++) {
            assertNotEquals(partitionManagersBefore.get(i).getPartition(), partitionManagersAfter.get(i).getPartition());
        }

    }

    private List<List<PartitionManager>> getPartitionManagers(List<ZkCoordinator> coordinatorList) {
        List<List<PartitionManager>> partitions = new ArrayList();
        for (ZkCoordinator coordinator : coordinatorList) {
            partitions.add(coordinator.getMyManagedPartitions());
        }
        return partitions;
    }

    private void waitForRefresh() throws InterruptedException {
        Thread.sleep(((ZkHosts) spoutConfig.hosts).refreshFreqSecs * 1000 + 1);
    }

    private List<ZkCoordinator> buildCoordinators(int totalTasks) {
        List<ZkCoordinator> coordinatorList = new ArrayList<ZkCoordinator>();
        for (int i = 0; i < totalTasks; i++) {
            ZkCoordinator coordinator = new ZkCoordinator(dynamicPartitionConnections, stormConf, spoutConfig, state, i, totalTasks, "test-id", reader);
            coordinatorList.add(coordinator);
        }
        return coordinatorList;
    }


}
