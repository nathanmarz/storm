package backtype.storm.security.auth.digest;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;

import org.apache.thrift7.transport.TSaslClientTransport;
import org.apache.thrift7.transport.TSaslServerTransport;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.SaslTransportPlugin;

public class DigestSaslTransportPlugin extends SaslTransportPlugin {
    public static final String DIGEST = "DIGEST-MD5";
    private static final Logger LOG = LoggerFactory.getLogger(DigestSaslTransportPlugin.class);

    protected TTransportFactory getServerTransportFactory() throws IOException {        
        //create an authentication callback handler
        CallbackHandler serer_callback_handler = new ServerCallbackHandler(login_conf);

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(DIGEST, AuthUtils.SERVICE, "localhost", null, serer_callback_handler);

        LOG.info("SASL DIGEST-MD5 transport factory will be used");
        return factory;
    }

    public TTransport connect(TTransport transport, String serverHost) throws TTransportException, IOException {
        ClientCallbackHandler client_callback_handler = new ClientCallbackHandler(login_conf);
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
