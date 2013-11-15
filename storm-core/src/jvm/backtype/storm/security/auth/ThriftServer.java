package backtype.storm.security.auth;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.security.auth.login.Configuration;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private Map _storm_conf; //storm configuration
    protected TProcessor _processor = null;
    private int _port = 0;
    private final Config.ThriftServerPurpose _purpose;
    private TServer _server = null;
    private Configuration _login_conf;
    private ExecutorService _executor_service;
    
    public ThriftServer(Map storm_conf, TProcessor processor, int port,
            Config.ThriftServerPurpose purpose) {
        this(storm_conf, processor, port, purpose, null);
    }
    
    public ThriftServer(Map storm_conf, TProcessor processor, int port,
            Config.ThriftServerPurpose purpose, 
            ExecutorService executor_service) {
        _storm_conf = storm_conf;
        _processor = processor;
        _port = port;
        _purpose = purpose;
        _executor_service = executor_service;

        try {
            //retrieve authentication configuration 
            _login_conf = AuthUtils.GetConfiguration(_storm_conf);
        } catch (Exception x) {
            LOG.error(x.getMessage(), x);
        }
    }

    public void stop() {
        if (_server != null)
            _server.stop();
    }

    /**
     * Is ThriftServer listening to requests?
     * @return
     */
    public boolean isServing() {
        if (_server == null) return false;
        return _server.isServing();
    }
    
    public void serve()  {
        try {
            //locate our thrift transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(_storm_conf, _login_conf, _executor_service);

            //server
            _server = transportPlugin.getServer(_port, _processor, _purpose);

            //start accepting requests
            _server.serve();
        } catch (Exception ex) {
            LOG.error("ThriftServer is being stopped due to: " + ex, ex);
            if (_server != null) _server.stop();
            Runtime.getRuntime().halt(1); //shutdown server process since we could not handle Thrift requests any more
        }
    }
}
