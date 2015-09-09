package backtype.storm.nimbus;

import backtype.storm.Config;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

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

    public static NimbusInfo fromConf(Map conf) {
        try {
            String host = InetAddress.getLocalHost().getCanonicalHostName();
            int port = Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT).toString());
            return new NimbusInfo(host, port, false);

        } catch (UnknownHostException e) {
            throw new RuntimeException("Something wrong with network/dns config, host cant figure out its name", e);
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

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NimbusInfo)) return false;

        NimbusInfo that = (NimbusInfo) o;

        if (isLeader != that.isLeader) return false;
        if (port != that.port) return false;
        if (!host.equals(that.host)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + (isLeader ? 1 : 0);
        return result;
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
