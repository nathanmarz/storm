package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicBrokersReader {
    
    CuratorFramework _curator;
    String _zkPath;
    String _topic;
    
    public DynamicBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        try {
            _zkPath = zkPath;
            _topic = topic;
            _curator = CuratorFrameworkFactory.newClient(
                    zkStr,
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    15000,
                    new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Map of host to List of port and number of partitions.
     * 
     * {"host1.mycompany.com" -> [9092, 5]}
     */
    public Map<String, List> getBrokerInfo() {     
        Map<String, List> ret = new HashMap();
        try {
            String topicBrokersPath = _zkPath + "/topics/" + _topic;
            String brokerInfoPath = _zkPath + "/ids";
            List<String> children = _curator.getChildren().forPath(topicBrokersPath);

            for(String c: children) {
                try {
                    byte[] numPartitionsData = _curator.getData().forPath(topicBrokersPath + "/" + c);
                    byte[] hostPortData = _curator.getData().forPath(brokerInfoPath + "/" + c);



                    HostPort hp = getBrokerHost(hostPortData);
                    int numPartitions = getNumPartitions(numPartitionsData);
                    List info = new ArrayList();
                    info.add((long)hp.port);
                    info.add((long)numPartitions);
                    ret.put(hp.host, info);
                    
                } catch(org.apache.zookeeper.KeeperException.NoNodeException e) {

                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
    
    public void close() {
        _curator.close();
    }
    
    private static HostPort getBrokerHost(byte[] contents) {
        try {
            String[] hostString = new String(contents, "UTF-8").split(":");
            String host = hostString[hostString.length - 2];
            int port = Integer.parseInt(hostString[hostString.length - 1]);
            return new HostPort(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }  
    
    private static int getNumPartitions(byte[] contents) {
        try {
            return Integer.parseInt(new String(contents, "UTF-8"));            
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    } 
}
