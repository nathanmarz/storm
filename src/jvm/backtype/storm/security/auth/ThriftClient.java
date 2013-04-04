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
    protected String _host;
    protected int _port;
    protected Integer _timeout;
    protected ITransportPlugin  _transportPlugin;

    public ThriftClient(Map storm_conf, String host, int port) throws TTransportException {
        this(storm_conf, host, port, null);
    }

    public ThriftClient(Map storm_conf, String host, int port, Integer timeout) throws TTransportException {
        //locate login configuration 
        Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

        //construct a transport plugin
        _transportPlugin = AuthUtils.GetTransportPlugin(storm_conf, login_conf, null);

        _host = host;
        if (host==null) 
            throw new IllegalArgumentException("host is not set");
        
        _port = port;
        if (port<=0) 
            throw new IllegalArgumentException("invalid port: "+port);
        
        _timeout = timeout; 
             
        //connect to server
        _transport = null;
        _protocol = null;
        connect(); 
    }

    protected void connect()  throws TTransportException {
        try {
            TSocket socket = new TSocket(_host, _port);
            if (_timeout!=null) 
                socket.setTimeout(_timeout);

            //establish client-server transport via plugin
            TTransport underlyingTransport = socket;
            _transport =  _transportPlugin.connect(underlyingTransport, _host); 

            _protocol = null;
            if (_transport != null)
                _protocol = new  TBinaryProtocol(_transport);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }   
    }
    
    public TTransport transport() {
        return _transport;
    }

    public void close() {
        if (_transport != null) 
            _transport.close();
        _transport = null;
    }
}
