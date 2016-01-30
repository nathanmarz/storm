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
package org.apache.storm.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class LeaderElectorImp implements ILeaderElector {
    private static Logger LOG = LoggerFactory.getLogger(LeaderElectorImp.class);
     private final Map conf;
     private final List<String> servers;
     private final CuratorFramework zk;
     private final String leaderlockPath;
     private final String id;
     private final AtomicReference<LeaderLatch> leaderLatch;
     private final AtomicReference<LeaderLatchListener> leaderLatchListener;

    public LeaderElectorImp(Map conf, List<String> servers, CuratorFramework zk, String leaderlockPath, String id, AtomicReference<LeaderLatch> leaderLatch,
            AtomicReference<LeaderLatchListener> leaderLatchListener) {
        this.conf = conf;
        this.servers = servers;
        this.zk = zk;
        this.leaderlockPath = leaderlockPath;
        this.id = id;
        this.leaderLatch = leaderLatch;
        this.leaderLatchListener = leaderLatchListener;
    }

    @Override
    public void prepare(Map conf) {
        // no-op for zookeeper implementation
    }

    @Override
    public void addToLeaderLockQueue() throws Exception {
        // if this latch is already closed, we need to create new instance.
        if (LeaderLatch.State.CLOSED.equals(leaderLatch.get().getState())) {
            leaderLatch.set(new LeaderLatch(zk, leaderlockPath));
            leaderLatchListener.set(Zookeeper.leaderLatchListenerImpl(conf, zk, leaderLatch.get()));
            LOG.info("LeaderLatch was in closed state. Resetted the leaderLatch and listeners.");
        }
        // Only if the latch is not already started we invoke start
        if (LeaderLatch.State.LATENT.equals(leaderLatch.get().getState())) {
            leaderLatch.get().addListener(leaderLatchListener.get());
            leaderLatch.get().start();
            LOG.info("Queued up for leader lock.");
        } else {
            LOG.info("Node already in queue for leader lock.");
        }
    }

    @Override
    // Only started latches can be closed.
    public void removeFromLeaderLockQueue() throws Exception {
        if (LeaderLatch.State.STARTED.equals(leaderLatch.get().getState())) {
            leaderLatch.get().close();
            LOG.info("Removed from leader lock queue.");
        } else {
            LOG.info("leader latch is not started so no removeFromLeaderLockQueue needed.");
        }
    }

    @Override
    public boolean isLeader() throws Exception {
        return leaderLatch.get().hasLeadership();
    }

    @Override
    public NimbusInfo getLeader() {
        try {
            return Zookeeper.toNimbusInfo(leaderLatch.get().getLeader());
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @Override
    public List<NimbusInfo> getAllNimbuses() throws Exception {
        List<NimbusInfo> nimbusInfos = new ArrayList<>();
        Collection<Participant> participants = leaderLatch.get().getParticipants();
        for (Participant participant : participants) {
            nimbusInfos.add(Zookeeper.toNimbusInfo(participant));
        }
        return nimbusInfos;
    }

    @Override
    public void close() {
        LOG.info("closing zookeeper connection of leader elector.");
        zk.close();
    }
}
