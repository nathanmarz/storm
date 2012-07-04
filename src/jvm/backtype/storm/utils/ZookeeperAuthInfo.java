package backtype.storm.utils;

import backtype.storm.Config;
import java.io.UnsupportedEncodingException;
import java.util.Map;


public class ZookeeperAuthInfo {
    public String scheme;
    public byte[] payload = null;
    
    public ZookeeperAuthInfo(Map conf) {
        String scheme = (String) conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME);
        String payload = (String) conf.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD);
        if(scheme!=null) {
            this.scheme = scheme;
            if(payload != null) {
                try {
                    this.payload = payload.getBytes("UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
    
    public ZookeeperAuthInfo(String scheme, byte[] payload) {
        this.scheme = scheme;
        this.payload = payload;
    }
}
