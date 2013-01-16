package backtype.storm.nimbus;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.utils.ZKPaths;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
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

            String mutexPath = (String) conf.get(Config.NIMBUS_ELECTIONS_ZOOKEEPER_PATH);

            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), mutexPath);
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

    public synchronized void awaitLeadership() throws Exception {
        mutex.acquire();
        hasLeadership = true;
    }

    public InetSocketAddress getLeaderAddr() {
        try {
            Collection<String> participantNodes = mutex.getParticipantNodes();
            if (participantNodes.size() > 0) {
                String leaderNode = participantNodes.iterator().next();
                return parseAdderss(participantForPath(leaderNode));
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't get leader's id", e);
        }
        return null;
    }

    public List<InetSocketAddress> getNimbusList() {
        try {
            Collection<String> participantNodes = mutex.getParticipantNodes();
            List<InetSocketAddress> result = Lists.newArrayList();
            if (participantNodes.size() > 0) {
                Iterator<String> iterator = participantNodes.iterator();
                while(iterator.hasNext()) {
                    result.add(parseAdderss(participantForPath(iterator.next())));
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Can't get participants list", e);
        }
    }

    private InetSocketAddress parseAdderss(String s) {
        String[] split = s.split(":");
        return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
    }

    public boolean hasLeadership() {
        return hasLeadership;
    }

    public synchronized void close() {
        if (hasLeadership()) {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new RuntimeException("Exception while releasing mutex", e);
            }
        }
        client.close();
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework createZkClient(Map conf) throws IOException {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        String connectString = Utils.makeZkConnectString(servers, port, "/");

        Integer retryTimes = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES));
        Integer retryInterval = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL));

        Integer connTimeout = Utils.getInt(conf.get(Config.NIMBUS_ELECTIONS_ZOOKEEPER_CONNECTION_TIMEOUT));
        Integer sessionTimeout = Utils.getInt(conf.get(Config.NIMBUS_ELECTIONS_ZOOKEEPER_SESSION_TIMEOUT));

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .connectionTimeoutMs(connTimeout)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new RetryNTimes(retryTimes, retryInterval)).build();
        client.start();

        return client;
    }

    private String participantForPath(String path) throws Exception {
        return new String(client.getData().forPath(path), "UTF-8");
    }
}
