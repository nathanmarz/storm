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
package org.apache.storm;

import org.apache.storm.cluster.PaceMakerStateStorage;
import org.apache.storm.generated.*;
import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

public class PaceMakerStateStorageFactoryTest {

    private class PaceMakerClientProxy extends PacemakerClient {
        private HBMessage response;
        private HBMessage captured;

        public PaceMakerClientProxy(HBMessage response, HBMessage captured) {
            this.response = response;
            this.captured = captured;
        }
        @Override
        public HBMessage send(HBMessage m) {
            captured = m;
            return response;
        }
        @Override
        public HBMessage checkCaptured() {
            return captured;
        }
    }

    @Test
    public void testSetWorkerHb() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.SEND_PULSE_RESPONSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.set_worker_hb("/foo", Utils.javaSerialize("data"), null);
        HBMessage sent = clientProxy.checkCaptured();
        HBPulse pulse = sent.get_data().get_pulse();
        Assert.assertEquals(HBServerMessageType.SEND_PULSE, sent.get_type());
        Assert.assertEquals("/foo", pulse.get_id());
        Assert.assertEquals("data", Utils.javaDeserialize(pulse.get_details(), String.class));
    }

    @Test(expected = RuntimeException.class)
    public void testSetWorkerHbResponseType() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.SEND_PULSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.set_worker_hb("/foo", Utils.javaSerialize("data"), null);
    }

    @Test
    public void testDeleteWorkerHb() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.DELETE_PATH_RESPONSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.delete_worker_hb("/foo/bar");
        HBMessage sent = clientProxy.checkCaptured();
        Assert.assertEquals(HBServerMessageType.DELETE_PATH, sent.get_type());
        Assert.assertEquals("/foo/bar", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteWorkerHbResponseType() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.DELETE_PATH, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.delete_worker_hb("/foo/bar");
    }

    @Test
    public void testGetWorkerHb() throws Exception {
        HBPulse hbPulse = new HBPulse();
        hbPulse.set_id("/foo");
        hbPulse.set_details(Utils.javaSerialize("some data"));
        HBMessage response = new HBMessage(HBServerMessageType.GET_PULSE_RESPONSE, HBMessageData.pulse(hbPulse));
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb("/foo", false);
        HBMessage sent = clientProxy.checkCaptured();
        Assert.assertEquals(HBServerMessageType.GET_PULSE, sent.get_type());
        Assert.assertEquals("/foo", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbBadResponse() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.GET_PULSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb("/foo", false);
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbBadData() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.GET_PULSE_RESPONSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb("/foo", false);
    }

    @Test
    public void testGetWorkerHbChildren() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, HBMessageData.nodes(new HBNodes()));
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb_children("/foo", false);
        HBMessage sent = clientProxy.checkCaptured();
        Assert.assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH, sent.get_type());
        Assert.assertEquals("/foo", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbChildrenBadResponse() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.DELETE_PATH, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb_children("/foo", false);
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbChildrenBadData() throws Exception {
        HBMessage response = new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, null);
        PaceMakerClientProxy clientProxy = new PaceMakerClientProxy(response, null);
        PaceMakerStateStorage stateStorage = new PaceMakerStateStorage(clientProxy, null);
        stateStorage.get_worker_hb_children("/foo", false);
    }

}
