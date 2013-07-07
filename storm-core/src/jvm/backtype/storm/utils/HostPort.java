package backtype.storm.utils;

import java.io.Serializable;

public class HostPort implements Serializable {
    private static final long serialVersionUID = -4903345013121505849L;
    String _host;
    int _port;
    
    public HostPort(String host, int port) {
        _host = host;
        _port = port;
    }
    
    public String host() {
        return _host;
    }
    
    public int port() {
        return _port;
    }
    
    public String toString() {
        return _host + ":" + _port;
    }
}
