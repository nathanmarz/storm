package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServerFactory;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import com.google.common.annotations.VisibleForTesting;

public class AnonymousAuthenticationProvider  extends java.security.Provider {
    public AnonymousAuthenticationProvider() {
	super("ThriftSaslAnonymous", 1.0, "Thrift Anonymous SASL provider");
	put("SaslClientFactory.ANONYMOUS", SaslAnonymousFactory.class.getName());
	put("SaslServerFactory.ANONYMOUS", SaslAnonymousFactory.class.getName());
    }

    public static class SaslAnonymousFactory implements SaslClientFactory, SaslServerFactory {
	@Override
	public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
					   String serverName, Map<String,?> props, CallbackHandler cbh)
	{
	    for (String mech : mechanisms) {
		if ("ANONYMOUS".equals(mech)) {
		    return new AnonymousClient(authorizationId);
		}
	    }
	    return null;
	}

	@Override
	public SaslServer createSaslServer(String mechanism, String protocol, 
					   String serverName, Map<String,?> props, CallbackHandler cbh)
	{
	    if ("ANONYMOUS".equals(mechanism)) {
		return new AnonymousServer();
	    }
	    return null;
	}
	public String[] getMechanismNames(Map<String, ?> props) {
	    return new String[] { "ANONYMOUS" };
	}
    }
}


class AnonymousClient implements SaslClient {
    @VisibleForTesting
    final String username;
    private boolean hasProvidedInitialResponse;

    public AnonymousClient(String username) {
	if (username == null) {
	    this.username = "anonymous";
	} else {
	    this.username = username;
	}
    }

    public String getMechanismName() { 
	return "ANONYMOUS"; 
    }

    public boolean hasInitialResponse() { 
	return true; 
    }

    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
	if (hasProvidedInitialResponse) {
	    throw new SaslException("Already complete!");
	}

	try {
	    hasProvidedInitialResponse = true;
	    return username.getBytes("UTF-8");
	} catch (IOException e) {
	    throw new SaslException(e.toString());
	}
    }

    public boolean isComplete() { 
	return hasProvidedInitialResponse; 
    }

    public byte[] unwrap(byte[] incoming, int offset, int len) {
	throw new UnsupportedOperationException();
    }

    public byte[] wrap(byte[] outgoing, int offset, int len) {
	throw new UnsupportedOperationException();
    }

    public Object getNegotiatedProperty(String propName) { 
	return null; 
    }

    public void dispose() {}
}

class AnonymousServer implements SaslServer {
    private String user;

    public String getMechanismName() { 
	return "ANONYMOUS"; 
    }

    public byte[] evaluateResponse(byte[] response) throws SaslException {
	try {
	    this.user = new String(response, "UTF-8");
	} catch (IOException e) {
	    throw new SaslException(e.toString());
	}
	return null;
    }

    public boolean isComplete() { 
	return user != null; 
    }

    public String getAuthorizationID() { 
	return user; 
    }

    public byte[] unwrap(byte[] incoming, int offset, int len) {
	throw new UnsupportedOperationException();
    }

    public byte[] wrap(byte[] outgoing, int offset, int len) {
	throw new UnsupportedOperationException();
    }

    public Object getNegotiatedProperty(String propName) { 
	return null; 
    }

    public void dispose() {}
}



