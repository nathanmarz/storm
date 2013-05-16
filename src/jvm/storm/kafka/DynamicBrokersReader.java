package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class DynamicBrokersReader {

	public static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

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
	 * Get all partitions with their current leaders
     */
    public GlobalPartitionInformation getBrokerInfo() {
		GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        try {
			int numPartitionsForTopic = getNumPartitions();
			String brokerInfoPath = _zkPath + "/ids";
			for (int partition = 0; partition < numPartitionsForTopic; partition++) {
				int leader = getLeaderFor(partition);
				String path = brokerInfoPath + "/" + leader;
				try {
					byte[] hostPortData = _curator.getData().forPath(path);
					HostPort hp = getBrokerHost(hostPortData);
					globalPartitionInformation.addPartition(partition, hp);
				} catch(org.apache.zookeeper.KeeperException.NoNodeException e) {
					LOG.error("Node {} does not exist ", path);
				}
			}
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
		LOG.info("Read partition info from zookeeper: " + globalPartitionInformation);
        return globalPartitionInformation;
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


	/**
	 * get /brokers/topics/distributedTopic/partitions/1/state
	 * { "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1, "version":1 }
	 * @param partition
	 * @return
	 */
	private int getLeaderFor(long partition) {
		try {
			String topicBrokersPath = _zkPath + "/topics/" + _topic + "/partitions";
			byte[] hostPortData = _curator.getData().forPath(topicBrokersPath + "/" + partition + "/state" );
			Map<Object, Object> value = (Map<Object,Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
			Integer leader = ((Number) value.get("leader")).intValue();
			return leader;
		} catch (Exception e) {
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
    private HostPort getBrokerHost(byte[] contents) {
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
