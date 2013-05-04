package backtype.storm.messaging.netty;

import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

public class Context implements IContext {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
    @SuppressWarnings("rawtypes")
    private Map storm_conf;
    private Vector<IConnection> server_connections;
    private Vector<IConnection> client_connections;
    
    /**
     * initialization per Storm configuration 
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map storm_conf) {
       this.storm_conf = storm_conf;
       server_connections = new Vector<IConnection>(); 
       client_connections = new Vector<IConnection>(); 
    }

    /**
     * establish a server with a binding port
     */
    public IConnection bind(String storm_id, int port) {
        IConnection server = new Server(storm_conf, port);
        server_connections.add(server);
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    public IConnection connect(String storm_id, String host, int port) {        
        IConnection client =  new Client(storm_conf, host, port);
        client_connections.add(client);
        return client;
    }

    /**
     * terminate this context
     */
    public void term() {
        for (IConnection client : client_connections) {
            client.close();
        }
        for (IConnection server : server_connections) {
            server.close();
        }
    }
}
