package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import java.util.*;
import java.util.Map.Entry;
import org.I0Itec.zkclient.ZkClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class KafkaSpoutState {
    ZkClient _zkClient;

    public KafkaSpoutState(List<String> zkServers) {
	_zkClient = new ZkClient(Utils.join(zkServers, ","), 10000);
    }

    public void writeData(String path, Map<String, Object> data) {
	if(!_zkClient.exists(path)) {
	    _zkClient.createPersistent(path, true);
	} 
	_zkClient.writeData(path, JSONValue.toJSONString(data));
    }

    public Map<String,Object> readData(String path) {
	if(!_zkClient.exists(path)) return null;
	return (Map<String,Object>)JSONValue.parse((String)_zkClient.readData(path));
    }
}
