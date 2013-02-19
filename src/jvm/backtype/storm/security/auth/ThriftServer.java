package backtype.storm.security.auth;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;
import java.io.IOException;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import org.apache.zookeeper.Login;
import org.apache.zookeeper.server.auth.KerberosName;
import org.apache.thrift7.TException;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.THsHaServer;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.server.TThreadPoolServer;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TNonblockingServerSocket;
import org.apache.thrift7.transport.TSaslServerTransport;
import org.apache.thrift7.transport.TServerSocket;
import org.apache.thrift7.transport.TServerTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.security.auth.*;
import backtype.storm.utils.Utils;

public class ThriftServer {
    private Map _storm_conf; //storm configuration
    private TProcessor _processor = null;
    private int _port = 0;
    private TServer _server;
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private String _loginConfigurationFile;

    public ThriftServer(Map storm_conf, TProcessor processor, int port) {
        try {
            _storm_conf = storm_conf;
            _processor = processor;
            _port = port;

            _loginConfigurationFile = System.getProperty("java.security.auth.login.config");
            if ((_loginConfigurationFile==null) || (_loginConfigurationFile.length()==0)) {
                //apply Storm configuration for JAAS login 
                Map conf = Utils.readStormConfig();
                _loginConfigurationFile = (String)conf.get("java.security.auth.login.config");
            }
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
            //retrieve authentication configuration 
            Configuration login_conf = AuthUtils.GetConfiguration(_storm_conf);

            //locate our thrift transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(_storm_conf, login_conf);

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
