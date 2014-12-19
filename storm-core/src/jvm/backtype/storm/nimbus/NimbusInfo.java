package backtype.storm.nimbus;

import java.io.Serializable;

public class NimbusInfo implements Serializable {
    private static final String DELIM = ":";

    private String host;
    private int port;
    private boolean isLeader;

    public NimbusInfo(String host, int port, boolean isLeader) {
        this.host = host;
        this.port = port;
        this.isLeader = isLeader;
    }

    public static NimbusInfo parse(String nimbusInfo) {
        String[] hostAndPort = nimbusInfo.split(DELIM);
        if(hostAndPort != null && hostAndPort.length == 2) {
            return new NimbusInfo(hostAndPort[0], Integer.parseInt(hostAndPort[1]), false);
        } else {
            throw new RuntimeException("nimbusInfo should have format of host:port, invalid string " + nimbusInfo);
        }
    }

    public String toHostPortString() {
        return String.format("%s%s%s",host,DELIM,port);
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
