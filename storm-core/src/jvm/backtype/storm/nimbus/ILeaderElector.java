package backtype.storm.nimbus;

import org.apache.curator.framework.CuratorFramework;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * The interface for leader election.
 */
public interface ILeaderElector {

    /**
     * Method guaranteed to be called as part of initialization of leader elector instance.
     * @param conf
     */
    void prepare(Map conf);

    /**
     * queue up for leadership lock. The call returns immediately and the caller must
     * check isLeader() to perform any leadership action.
     */
    void addToLeaderLockQueue() throws Exception;

    /**
     * Removes the caller from the leader lock queue. If the caller is leader
     * also releases the lock.
     */
    void removeFromLeaderLockQueue() throws Exception;

    /**
     *
     * @return true if the caller currently has the leader lock.
     */
    boolean isLeader() throws Exception;

    /**
     *
     * @return the current leader's address , may return null if no
     */
    InetSocketAddress getLeaderAddress() throws Exception;

    /**
     *
     * @return list of current nimbus addresses, includes leader.
     */
    List<InetSocketAddress> getAllNimbusAddresses() throws Exception;

    /**
     * Method called to allow for cleanup. once close this object can not be reused.
     */
    void close() throws Exception;

}

