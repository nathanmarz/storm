package storm.kafka;

import java.io.Serializable;

public class HostPort implements Serializable {
    public String host;
    public int port;
    
    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public HostPort(String host) {
        this(host, 9092);
    }
}
