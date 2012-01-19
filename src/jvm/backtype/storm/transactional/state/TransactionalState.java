package backtype.storm.transactional.state;

import backtype.storm.Config;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;

public class TransactionalState {
    CuratorFramework _curator;
    KryoValuesSerializer _ser;
    KryoValuesDeserializer _des;
    
    public static TransactionalState newUserState(Map conf, String id, ITransactionalSpout spout) {
        return new TransactionalState(conf, id, spout, "user");
    }
    
    public static TransactionalState newCoordinatorState(Map conf, String id, ITransactionalSpout spout) {
        return new TransactionalState(conf, id, spout, "coordinator");        
    }
    
    protected TransactionalState(Map conf, String id, ITransactionalSpout spout, String subroot) {
        try {
            conf = new HashMap(conf);
            // ensure that the serialization registrations are consistent with the declarations in this spout
            if(spout.getComponentConfiguration()!=null) {
                conf.put(Config.TOPOLOGY_KRYO_REGISTER,
                         spout.getComponentConfiguration()
                              .get(Config.TOPOLOGY_KRYO_REGISTER));
            }
            String rootDir = conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT) + "/" + id + "/" + subroot;
            CuratorFramework initter = Utils.newCurator(conf);
            initter.create().creatingParentsIfNeeded().forPath(rootDir);
            initter.close();
                                    
            _curator = Utils.newCurator(conf, rootDir);
            _ser = new KryoValuesSerializer(conf);
            _des = new KryoValuesDeserializer(conf);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }
    
    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser = _ser.serializeObject(obj);
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
                return _des.deserializeObject(_curator.getData().forPath(path));
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
}
