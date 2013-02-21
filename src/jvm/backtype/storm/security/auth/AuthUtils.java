package backtype.storm.security.auth;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class AuthUtils {
    public static final String LOGIN_CONTEXT_SERVER = "StormServer"; 
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient"; 
    public static final String SERVICE = "storm_thrift_server";
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);

    /**
     * Construct a JAAS configuration object per storm configuration file
     * @param storm_conf Storm configuration 
     * @return
     */
    public static synchronized Configuration GetConfiguration(Map storm_conf) {
        Configuration.setConfiguration(null);

        //exam system property first
        String orig_loginConfigurationFile = System.getProperty("java.security.auth.login.config");

        //try to find login file from Storm configuration  
        String loginConfigurationFile = (String)storm_conf.get("java.security.auth.login.config");
        if (loginConfigurationFile==null)
            loginConfigurationFile = orig_loginConfigurationFile;

        Configuration login_conf = null;
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length()>0)) { 
            System.setProperty("java.security.auth.login.config", loginConfigurationFile);
            login_conf =  Configuration.getConfiguration();
            if (orig_loginConfigurationFile!=null)
                System.setProperty("java.security.auth.login.config", orig_loginConfigurationFile);
        }
        return login_conf;
    }

    /**
     * Construct a transport plugin per storm configuration
     * @param conf storm configuration
     * @return
     */
    public static ITransportPlugin GetTransportPlugin(Map storm_conf, Configuration login_conf) {
        ITransportPlugin  transportPlugin = null;
        try {
            String transport_plugin_klassName = (String) storm_conf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN);
            Class klass = Class.forName(transport_plugin_klassName);
            transportPlugin = (ITransportPlugin)klass.getConstructor(Configuration.class).newInstance(login_conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        } 
        return transportPlugin;
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

