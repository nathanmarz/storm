package backtype.storm.messaging.zmq;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class TransportPlugin implements IContext {
    public static final Logger LOG = LoggerFactory.getLogger(TransportPlugin.class);

    private Context context; 
    private long linger_ms, hwm;
    private boolean isLocal;
            
    public void prepare(Map storm_conf) {
        LOG.debug("zmq.TranportPlugin:prepare()");
        int num_threads = (Integer)storm_conf.get(Config.ZMQ_THREADS);
        context = ZMQ.context(num_threads);
        linger_ms = (Long)storm_conf.get(Config.ZMQ_LINGER_MILLIS);
        hwm = (Integer)storm_conf.get(Config.ZMQ_HWM); 
        isLocal = new String("local").equals((String)storm_conf.get(Config.STORM_CLUSTER_MODE));
    }   
    
    public void term() {
        LOG.debug("zmq.TranportPlugin:term()");
        if (context!=null) {
            context.term();
            context = null;
        }
    }
    
    public IConnection bind(String storm_id, int port) {
        LOG.debug("zmq.TranportPlugin:bind()");
        Socket socket = context.socket(ZMQ.PULL);
        socket.setHWM(hwm);
        socket.setLinger(linger_ms);
        socket.bind(bindUrl(port));
        return new Connection(socket);
    }
    
    public IConnection connect(String storm_id, String host, int port) {
        LOG.debug("zmq.TranportPlugin:connect()");
        Socket socket = context.socket(ZMQ.PUSH);
        socket.setHWM(hwm);
        socket.setLinger(linger_ms);
        socket.connect(connectionUrl(host, port));
        return new Connection(socket);
    }
    
    private String bindUrl(int port) {
        if (isLocal) 
            return "ipc://"+port+".ipc";
        return "tcp://*:"+port;
    }
 
    private String connectionUrl(String host, int port) {
        if (isLocal) 
            return "ipc://"+port+".ipc";
        return "tcp://"+host+":"+port;
    }
}
