package backtype.storm.nimbus;

import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * The interface for leader election.
 */
public interface ILeaderElector extends Closeable {

    /**
     * Method guaranteed to be called as part of initialization of leader elector instance.
     * @param conf
     */
    void prepare(Map conf);

    /**
     * queue up for leadership lock. The call returns immediately and the caller must
     * check isLeader() to perform any leadership action.
     */
    void addToLeaderLockQueue();

    /**
     * Removes the caller from the leader lock queue. If the caller is leader
     * also releases the lock.
     */
    void removeFromLeaderLockQueue();

    /**
     *
     * @return true if the caller currently has the leader lock.
     */
    boolean isLeader() throws Exception;

    /**
     *
     * @return the current leader's address , may return null if no one has the lock.
     */
    NimbusInfo getLeader();

    /**
     *
     * @return list of current nimbus addresses, includes leader.
     */
    List<NimbusInfo> getAllNimbuses();

    /**
     * Method called to allow for cleanup. once close this object can not be reused.
     */
    @Override
    void close();
}

