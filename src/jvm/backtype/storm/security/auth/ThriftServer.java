package backtype.storm.security.auth;

import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.utils.Utils;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private Map _storm_conf; //storm configuration
    private TProcessor _processor = null;
    private int _port = 0;
    private TServer _server;
    private Configuration _login_conf;
    
    public ThriftServer(Map storm_conf, TProcessor processor, int port) {
        try {
            _storm_conf = storm_conf;
            _processor = processor;
            _port = port;
            
            //retrieve authentication configuration 
            _login_conf = AuthUtils.GetConfiguration(_storm_conf);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public void stop() {
        if (_server != null)
            _server.stop();
    }

    public void serve()  {
        try {
            //locate our thrift transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(_storm_conf, _login_conf);

            //server
            _server = transportPlugin.getServer(_port, _processor);

            //start accepting requests
            _server.serve();
        } catch (Exception ex) {
            LOG.error("ThriftServer is being stopped due to: " + ex, ex);
            if (_server != null) _server.stop();
            System.exit(1); //shutdown server process since we could not handle Thrift requests any more
        }
    }
}
