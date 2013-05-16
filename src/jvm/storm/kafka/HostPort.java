package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HostPort implements Serializable, Comparable<HostPort> {
    public String host;
    public int port;
    
    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public HostPort(String host) {
        this(host, 9092);
    }

    @Override
    public boolean equals(Object o) {
        HostPort other = (HostPort) o;
        return host.equals(other.host) && port == other.port;
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

	public static HostPort fromString(String host) {
		HostPort hp;
		String[] spec = host.split(":");
		if (spec.length == 1) {
			hp = new HostPort(spec[0]);
		} else if (spec.length == 2) {
			hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
		} else {
			throw new IllegalArgumentException("Invalid host specification: " + host);
		}
		return hp;
	}


	@Override
	public int compareTo(HostPort o) {
		if ( this.host.equals(o.host)) {
			return this.port - o.port;
		} else {
			return this.host.compareTo(o.host);
		}
	}
}
