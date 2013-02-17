package backtype.storm.security.auth;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class AuthUtils {
	public static String LoginContextServer = "StormServer"; 
	public static String LoginContextClient = "StormClient"; 
	public static final String DIGEST = "DIGEST-MD5";
	public static final String ANONYMOUS = "ANONYMOUS";
	public static final String KERBEROS = "GSSAPI"; 
	public static final String SERVICE = "storm_thrift_server";
	private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);

	public static synchronized Configuration getConfiguration(String loginConfigurationFile) {
		Configuration.setConfiguration(null);
		System.setProperty("java.security.auth.login.config", loginConfigurationFile);
		return  Configuration.getConfiguration();
	}

	public static String get(Configuration configuration, String section, String key) throws IOException {
		AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(section);
		if (configurationEntries == null) {
			String errorMessage = "Could not find a '"+ section + "' entry in this configuration.";
			LOG.error(errorMessage);
			throw new IOException(errorMessage);
		}

		for(AppConfigurationEntry entry: configurationEntries) {
			Object val = entry.getOptions().get(key); 
			if (val != null) 
				return (String)val;
		}
		return null;
	}
}

