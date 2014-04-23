/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.security.auth;

import backtype.storm.Config;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class AuthUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);
    public static final String LOGIN_CONTEXT_SERVER = "StormServer"; 
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient"; 
    public static final String SERVICE = "storm_thrift_server";

    /**
     * Construct a JAAS configuration object per storm configuration file 
     * @param storm_conf Storm configuration 
     * @return JAAS configuration object
     */
    public static Configuration GetConfiguration(Map storm_conf) {
        Configuration login_conf = null;

        //find login file configuration from Storm configuration  
        String loginConfigurationFile = (String)storm_conf.get("java.security.auth.login.config");
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length()>0)) { 
            try {
                URI config_uri = new File(loginConfigurationFile).toURI();
                login_conf = Configuration.getInstance("JavaLoginConfig", new URIParameter(config_uri));
            } catch (NoSuchAlgorithmException ex1) {
                if (ex1.getCause() instanceof FileNotFoundException)
                    throw new RuntimeException("configuration file "+loginConfigurationFile+" could not be found");
                else throw new RuntimeException(ex1);
            } catch (Exception ex2) {
                throw new RuntimeException(ex2);
            }
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
            transportPlugin = (ITransportPlugin)klass.newInstance();
            transportPlugin.prepare(storm_conf, login_conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        } 
        return transportPlugin;
    }

    public static String get(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+ section + "' entry in this configuration.";
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

