package backtype.storm.security.auth;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

public class AuthUtils {
	public static String LoginContextServer = "StormServer"; 
	public static String LoginContextClient = "StormClient"; 

	static public final String DIGEST = "DIGEST-MD5";
	static public final String ANONYMOUS = "ANONYMOUS";
	static public final String KERBEROS = "GSSAPI"; 
	static public final String SERVICE = "storm_thrift_server";

	private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);

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

