/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.common;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class TransportAddressesTest {

    @Test
    public void readsMultipleHosts() throws Exception {
        String[] hosts = new String[] {"h1:1000", "h2:10003"};
        TransportAddresses addresses = new TransportAddresses(hosts);
        assertThat(addresses, containsInAnyOrder(new InetSocketTransportAddress("h1", 1000),
                                                 new InetSocketTransportAddress("h2", 10003)));
    }

    @Test
    public void stripsSpaces() throws Exception {
        String[] hosts = new String[] {"h1:1000", " h2:10003 "};
        TransportAddresses addresses = new TransportAddresses(hosts);
        assertThat(addresses, containsInAnyOrder(new InetSocketTransportAddress("h1", 1000),
                                                 new InetSocketTransportAddress("h2", 10003)));
    }

    @Test
    public void readsOneHost() throws Exception {
        String[] hosts = new String[] {"h1:1000"};
        TransportAddresses addresses = new TransportAddresses(hosts);
        assertThat(addresses, containsInAnyOrder(new InetSocketTransportAddress("h1", 1000)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnNullHosts() throws Exception {
        new TransportAddresses(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnEmptyArray() throws Exception {
        new TransportAddresses(new String[] {}).iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnInvalidHostAndPortPair() throws Exception {
        new TransportAddresses(new String[] {"h1:1000", "h2"}).iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnInvalidPortValue() throws Exception {
        new TransportAddresses(new String[] {"h1:-1000"}).iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnPortNotANumber() throws Exception {
        new TransportAddresses(new String[] {"h1:dummy"}).iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnInvalidHostAndPortFormat() throws Exception {
        new TransportAddresses(new String[] {"h1:dummy:231"}).iterator();
    }

}
