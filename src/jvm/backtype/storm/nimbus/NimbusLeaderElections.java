package backtype.storm.nimbus;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.RetryNTimes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author slukjanov
 */
public class NimbusLeaderElections {
    private CuratorFramework client;
    private InterProcessMutex mutex;
    private volatile boolean hasLeadership;

    public NimbusLeaderElections() {
    }

    public void init(final Map conf, final String id) {
        try {
            Preconditions.checkNotNull(conf);
            client = createZkClient(conf);

            String mutexPath = (String) conf.get(Config.NIMBUS_ELECTIONS_ZOOKEEPER_ROOT);

            mutex = new InterProcessMutex(client, mutexPath) {
                @Override
                protected byte[] getLockNodeBytes() {
                    try {
                        return id == null ? null : id.getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException("UTF-8 isn't supported", e);
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException("Exception while initializing NimbusLeaderElections", e);
        }
    }

    public void awaitLeadership() throws Exception {
        mutex.acquire();
        hasLeadership = true;
    }

    public String getLeaderId() {
        try {
            Collection<String> participantNodes = mutex.getParticipantNodes();
            if (participantNodes.size() > 0) {
                String leaderNode = participantNodes.iterator().next();
                return participantForPath(leaderNode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't get leader's id", e);
        }
        return null;
    }

    public boolean hasLeadership() {
        return hasLeadership;
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework createZkClient(Map conf) throws IOException {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        String root = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);
        String connectString = Utils.makeZkConnectString(servers, port, root);

        Integer retryTimes = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES));
        Integer retryInterval = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL));

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
                .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
                .retryPolicy(new RetryNTimes(retryTimes, retryInterval)).build();
        client.start();

        return client;
    }

    private String participantForPath(String path) throws Exception {
        return new String(client.getData().forPath(path), "UTF-8");
    }
}
