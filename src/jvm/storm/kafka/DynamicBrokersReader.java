package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicBrokersReader {
    
    private CuratorFramework _curator;
    private String _zkPath;
    private String _topic;
    
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
	 *
	 * TODO: support multiple ports per host
     */
    public Map<String, List> getBrokerInfo() {     
        Map<String, List> ret = new HashMap();
        try {
            String brokerInfoPath = _zkPath + "/ids";
            List<String> brokerIds = _curator.getChildren().forPath(brokerInfoPath);
            for(String brokerId: brokerIds) {
                try {
                    byte[] hostPortData = _curator.getData().forPath(brokerInfoPath + "/" + brokerId);
                    HostPort hp = getBrokerHost(hostPortData);
                    int numPartitions = getNumPartitions();
                    List info = new ArrayList();
                    info.add((long)hp.port);
                    info.add((long)numPartitions);
                    ret.put(hp.host, info);
                } catch(org.apache.zookeeper.KeeperException.NoNodeException e) {
			   		e.printStackTrace();
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

	private int getNumPartitions() {
		try {
			String topicBrokersPath = _zkPath + "/topics/" + _topic + "/partitions";
			List<String> children = _curator.getChildren().forPath(topicBrokersPath);
			return children.size();
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
    
    public void close() {
        _curator.close();
    }

	/**
	 *
	 * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0
	 * { "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
	 *
	 * @param contents
	 * @return
	 */
    private static HostPort getBrokerHost(byte[] contents) {
        try {
			Map<Object, Object> value = (Map<Object,Object>) JSONValue.parse(new String(contents, "UTF-8"));
			String host = (String) value.get("host");
			Integer port = ((Long) value.get("port")).intValue();
            return new HostPort(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }  

}
