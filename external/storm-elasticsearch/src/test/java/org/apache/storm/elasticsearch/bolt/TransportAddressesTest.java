package org.apache.storm.elasticsearch.bolt;

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
