package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL client side callback handler.
 */
public class SaslClientCallbackHandler implements CallbackHandler {
	private static final String USERNAME = "username";
	private static final String PASSWORD = "password";
	private static final Logger LOG = LoggerFactory.getLogger(SaslClientCallbackHandler.class);
	private String _username = null;
	private String _password = null;

	/**
	 * Constructor based on a JAAS configuration
	 * 
	 * For digest, you should have a pair of user name and password defined in this figgure.
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	public SaslClientCallbackHandler(Configuration configuration) throws IOException {
		if (configuration == null) return;
		AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtils.LoginContextClient);
		if (configurationEntries == null) {
			String errorMessage = "Could not find a '"+AuthUtils.LoginContextClient
					+ "' entry in this configuration: Client cannot start.";
			LOG.error(errorMessage);
			throw new IOException(errorMessage);
		}

		for(AppConfigurationEntry entry: configurationEntries) {
			if (entry.getOptions().get(USERNAME) != null) {
				_username = (String)entry.getOptions().get(USERNAME);
			}
			if (entry.getOptions().get(PASSWORD) != null) {
				_password = (String)entry.getOptions().get(PASSWORD);
			}
		}
	}

	/**
	 * This method is invoked by SASL for authentication challenges
	 * @param callbacks a collection of challenge callbacks 
	 */
	public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
		for (Callback c : callbacks) {
			if (c instanceof NameCallback) {
				LOG.debug("name callback");
				NameCallback nc = (NameCallback) c;
				nc.setName(_username);
			} else if (c instanceof PasswordCallback) {
				LOG.debug("password callback");
				PasswordCallback pc = (PasswordCallback)c;
				if (_password != null) {
					pc.setPassword(_password.toCharArray());
				} else {
					LOG.warn("Could not login: the client is being asked for a password, but the " +
							" client code does not currently support obtaining a password from the user." +
							" Make sure that the client is configured to use a ticket cache (using" +
							" the JAAS configuration setting 'useTicketCache=true)' and restart the client. If" +
							" you still get this message after that, the TGT in the ticket cache has expired and must" +
							" be manually refreshed. To do so, first determine if you are using a password or a" +
							" keytab. If the former, run kinit in a Unix shell in the environment of the user who" +
							" is running this client using the command" +
							" 'kinit <princ>' (where <princ> is the name of the client's Kerberos principal)." +
							" If the latter, do" +
							" 'kinit -k -t <keytab> <princ>' (where <princ> is the name of the Kerberos principal, and" +
							" <keytab> is the location of the keytab file). After manually refreshing your cache," +
							" restart this client. If you continue to see this message after manually refreshing" +
							" your cache, ensure that your KDC host's clock is in sync with this host's clock.");
				}
			} else if (c instanceof AuthorizeCallback) {
				LOG.debug("authorization callback");
				AuthorizeCallback ac = (AuthorizeCallback) c;
				String authid = ac.getAuthenticationID();
				String authzid = ac.getAuthorizationID();
				if (authid.equals(authzid)) {
					ac.setAuthorized(true);
				} else {
					ac.setAuthorized(false);
				}
				if (ac.isAuthorized()) {
					ac.setAuthorizedID(authzid);
				}
			} else if (c instanceof RealmCallback) {
				RealmCallback rc = (RealmCallback) c;
				((RealmCallback) c).setText(rc.getDefaultText());
			} else {
				throw new UnsupportedCallbackException(c);
			}
		}
	}
}
