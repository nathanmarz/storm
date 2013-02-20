package backtype.storm.security.auth;

import java.io.IOException;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;

import org.apache.thrift7.transport.TSaslClientTransport;
import org.apache.thrift7.transport.TSaslServerTransport;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DigestSaslTransportPlugin extends SaslTransportPlugin {
    public static final String DIGEST = "DIGEST-MD5";
    private static final Logger LOG = LoggerFactory.getLogger(DigestSaslTransportPlugin.class);

    /**
     * constructor
     */
    public DigestSaslTransportPlugin(Configuration login_conf) {
        super(login_conf);
    }

    protected TTransportFactory getServerTransportFactory() throws IOException {        
        //create an authentication callback handler
        CallbackHandler serer_callback_handler = new SaslServerCallbackHandler(login_conf);

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(DIGEST, AuthUtils.SERVICE, "localhost", null, serer_callback_handler);

        LOG.info("SASL DIGEST-MD5 transport factory will be used:"+login_conf);
        return factory;
    }

    public TTransport connect(TTransport transport, String serverHost) throws TTransportException, IOException {
        SaslClientCallbackHandler client_callback_handler = new SaslClientCallbackHandler(login_conf);
        TSaslClientTransport wrapper_transport = new TSaslClientTransport(DIGEST, 
                null, 
                AuthUtils.SERVICE, 
                serverHost,
                null,
                client_callback_handler, 
                transport);

        wrapper_transport.open();
        LOG.debug("SASL DIGEST-MD5 client transport has been established");

        return wrapper_transport;
    }

}
