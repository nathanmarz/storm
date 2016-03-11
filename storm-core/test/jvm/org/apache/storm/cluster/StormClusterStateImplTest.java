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
package org.apache.storm.cluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;
import org.mockito.Matchers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.KeeperException;

import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.cluster.ClusterStateContext;

public class StormClusterStateImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(StormClusterStateImplTest.class);
    private final String[] pathlist = { ClusterUtils.ASSIGNMENTS_SUBTREE, 
                                        ClusterUtils.STORMS_SUBTREE, 
                                        ClusterUtils.SUPERVISORS_SUBTREE, 
                                        ClusterUtils.WORKERBEATS_SUBTREE,
                                        ClusterUtils.ERRORS_SUBTREE, 
                                        ClusterUtils.BLOBSTORE_SUBTREE, 
                                        ClusterUtils.NIMBUSES_SUBTREE, 
                                        ClusterUtils.LOGCONFIG_SUBTREE,
                                        ClusterUtils.BACKPRESSURE_SUBTREE };

    private IStateStorage storage;
    private ClusterStateContext context;
    private StormClusterStateImpl state;

    @Before
    public void init() throws Exception {
        storage = Mockito.mock(IStateStorage.class);
        context = new ClusterStateContext();
        state = new StormClusterStateImpl(storage, null /*acls*/, context, false /*solo*/);
    }


    @Test
    public void registeredCallback() {
        Mockito.verify(storage).register(Matchers.<ZKStateChangedCallback>anyObject());
    }

    @Test
    public void createdZNodes() {
        for (String path : pathlist) {
            Mockito.verify(storage).mkdirs(path, null);
        }
    }

    @Test
    public void removeBackpressureDoesNotThrowTest() {
        // setup to throw
        Mockito.doThrow(new RuntimeException(new KeeperException.NoNodeException("foo")))
               .when(storage)
               .delete_node(Matchers.anyString());
        try {
            state.removeBackpressure("bogus-topo-id");
            // teardown backpressure should have caught the exception
            Mockito.verify(storage)
                   .delete_node(ClusterUtils.backpressureStormRoot("bogus-topo-id"));
        } catch (Exception e) {
            Assert.fail("Exception thrown when it shouldn't have: " + e);
        }
    }

    @Test
    public void removeWorkerBackpressureDoesntAttemptForNonExistentZNodeTest() {
        // setup to throw
        Mockito.when(storage.node_exists(Matchers.anyString(), Matchers.anyBoolean()))
               .thenReturn(false);

        state.removeWorkerBackpressure("bogus-topo-id", "bogus-host", new Long(1234));

        Mockito.verify(storage, Mockito.never())
               .delete_node(Matchers.anyString());
    }

    @Test
    public void removeWorkerBackpressureCleansForExistingZNodeTest() {
        // setup to throw
        Mockito.when(storage.node_exists(Matchers.anyString(), Matchers.anyBoolean()))
               .thenReturn(true);

        state.removeWorkerBackpressure("bogus-topo-id", "bogus-host", new Long(1234));

        Mockito.verify(storage)
               .delete_node(ClusterUtils.backpressurePath("bogus-topo-id", "bogus-host", new Long(1234)));
    }
}

