package backtype.storm.security.auth;

import java.io.IOException;
import javax.security.auth.login.Configuration;
import org.apache.thrift7.transport.TSaslClientTransport;
import org.apache.thrift7.transport.TSaslServerTransport;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnonymousSaslTransportPlugin extends SaslTransportPlugin {
    static {
        java.security.Security.addProvider(new AnonymousAuthenticationProvider());
    }

    public static final String ANONYMOUS = "ANONYMOUS";
    private static final Logger LOG = LoggerFactory.getLogger(AnonymousSaslTransportPlugin.class);

    public AnonymousSaslTransportPlugin(Configuration login_conf) {
        super(login_conf);
    }

    public TTransportFactory getServerTransportFactory() throws IOException {        
        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(ANONYMOUS, AuthUtils.SERVICE, "localhost", null, null);
        LOG.info("SASL ANONYMOUS transport factory will be used");
        return factory;
    }

    public TTransport connect(TTransport transport, String serverHost) 
            throws TTransportException, IOException {
        TSaslClientTransport wrapper_transport = new TSaslClientTransport(ANONYMOUS, 
                null, 
                AuthUtils.SERVICE, 
                serverHost,
                null,
                null, 
                transport);
        LOG.debug("SASL ANONYMOUS client transport is being established");
        wrapper_transport.open();
        return wrapper_transport;
    }
}
