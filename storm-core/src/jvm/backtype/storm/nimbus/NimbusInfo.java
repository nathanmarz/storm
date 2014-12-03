package backtype.storm.nimbus;

import java.io.Serializable;

public class NimbusInfo implements Serializable {
    private String host;
    private int port;
    private transient boolean isLeader;

    public NimbusInfo(String host, int port, boolean isLeader) {
        this.host = host;
        this.port = port;
        this.isLeader = isLeader;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "NimbusInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", isLeader=" + isLeader +
                '}';
    }
}
