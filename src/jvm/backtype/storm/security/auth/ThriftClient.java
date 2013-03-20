package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.utils.Utils;

public class ThriftClient {	
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);
    private TTransport _transport;
    protected TProtocol _protocol;

    public ThriftClient(Map storm_conf, String host, int port) throws TTransportException {
        this(storm_conf, host, port, null);
    }

    public ThriftClient(Map storm_conf, String host, int port, Integer timeout) throws TTransportException {
        try {
            //locate login configuration 
            Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

            //construct a transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(storm_conf, login_conf, null);

            //create a socket with server
            if(host==null) {
                throw new IllegalArgumentException("host is not set");
            }
            if(port<=0) {
                throw new IllegalArgumentException("invalid port: "+port);
            }            
            TSocket socket = new TSocket(host, port);
            if(timeout!=null) {
                socket.setTimeout(timeout);
            }
            final TTransport underlyingTransport = socket;

            //establish client-server transport via plugin
            _transport =  transportPlugin.connect(underlyingTransport, host); 
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        _protocol = null;
        if (_transport != null)
            _protocol = new  TBinaryProtocol(_transport);
    }

    public TTransport transport() {
        return _transport;
    }

    public void close() {
        _transport.close();
    }
}
