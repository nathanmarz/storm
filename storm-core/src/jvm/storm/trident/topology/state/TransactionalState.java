package storm.trident.topology.state;


import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONValue;

public class TransactionalState {
    CuratorFramework _curator;
    
    public static TransactionalState newUserState(Map conf, String id) {
        return new TransactionalState(conf, id, "user");
    }
    
    public static TransactionalState newCoordinatorState(Map conf, String id) {
        return new TransactionalState(conf, id, "coordinator");        
    }
    
    protected TransactionalState(Map conf, String id, String subroot) {
        try {
            conf = new HashMap(conf);
            String rootDir = conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT) + "/" + id + "/" + subroot;
            List<String> servers = (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Config.STORM_ZOOKEEPER_SERVERS);
            Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            CuratorFramework initter = Utils.newCuratorStarted(conf, servers, port);
            try {
                initter.create().creatingParentsIfNeeded().forPath(rootDir);
            } catch(KeeperException.NodeExistsException e)  {
                
            }
            
            initter.close();
                                    
            _curator = Utils.newCuratorStarted(conf, servers, port, rootDir);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }
    
    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser;
        try {
            ser = JSONValue.toJSONString(obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        try {
            if(_curator.checkExists().forPath(path)!=null) {
                _curator.setData().forPath(path, ser);
            } else {
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, ser);
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void delete(String path) {
        path = "/" + path;
        try {
            _curator.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public List<String> list(String path) {
        path = "/" + path;
        try {
            if(_curator.checkExists().forPath(path)==null) {
                return new ArrayList<String>();
            } else {
                return _curator.getChildren().forPath(path);
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }   
    }
    
    public void mkdir(String path) {
        setData(path, 7);
    }
    
    public Object getData(String path) {
        path = "/" + path;
        try {
            if(_curator.checkExists().forPath(path)!=null) {
                return JSONValue.parse(new String(_curator.getData().forPath(path), "UTF-8"));
            } else {
                return null;
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close() {
        _curator.close();
    }
    
    private Object getWithBackup(Map amap, Object primary, Object backup) {
        Object ret = amap.get(primary);
        if(ret==null) return amap.get(backup);
        return ret;
    }
}
