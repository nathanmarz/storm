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
import javax.security.auth.Subject;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;

import backtype.storm.security.INimbusCredentialPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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
            File config_file = new File(loginConfigurationFile);
            if (! config_file.canRead()) {
                throw new RuntimeException("File " + loginConfigurationFile +
                        " cannot be read.");
            }
            try {
                URI config_uri = config_file.toURI();
                login_conf = Configuration.getInstance("JavaLoginConfig", new URIParameter(config_uri));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return login_conf;
    }

    /**
     * Construct a principal to local plugin
     * @param conf storm configuration
     * @return the plugin
     */
    public static IPrincipalToLocal GetPrincipalToLocalPlugin(Map storm_conf) {
        IPrincipalToLocal ptol = null;
        try {
          String ptol_klassName = (String) storm_conf.get(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN);
          Class klass = Class.forName(ptol_klassName);
          ptol = (IPrincipalToLocal)klass.newInstance();
          ptol.prepare(storm_conf);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return ptol;
    }

    /**
     * Construct a group mapping service provider plugin
     * @param conf storm configuration
     * @return the plugin
     */
    public static IGroupMappingServiceProvider GetGroupMappingServiceProviderPlugin(Map storm_conf) {
        IGroupMappingServiceProvider gmsp = null;
        try {
            String gmsp_klassName = (String) storm_conf.get(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN);
            Class klass = Class.forName(gmsp_klassName);
            gmsp = (IGroupMappingServiceProvider)klass.newInstance();
            gmsp.prepare(storm_conf);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return gmsp;
    }

    /**
     * Get all of the configured Credential Renwer Plugins.
     * @param storm_conf the storm configuration to use.
     * @return the configured credential renewers.
     */
    public static Collection<ICredentialsRenewer> GetCredentialRenewers(Map conf) {
        try {
            Set<ICredentialsRenewer> ret = new HashSet<ICredentialsRenewer>();
            Collection<String> clazzes = (Collection<String>)conf.get(Config.NIMBUS_CREDENTIAL_RENEWERS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    ICredentialsRenewer inst = (ICredentialsRenewer)Class.forName(clazz).newInstance();
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all the Nimbus Auto cred plugins.
     * @param conf nimbus configuration to use.
     * @return nimbus auto credential plugins.
     */
    public static Collection<INimbusCredentialPlugin> getNimbusAutoCredPlugins(Map conf) {
        try {
            Set<INimbusCredentialPlugin> ret = new HashSet<INimbusCredentialPlugin>();
            Collection<String> clazzes = (Collection<String>)conf.get(Config.NIMBUS_AUTO_CRED_PLUGINS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    INimbusCredentialPlugin inst = (INimbusCredentialPlugin)Class.forName(clazz).newInstance();
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all of the configured AutoCredential Plugins.
     * @param storm_conf the storm configuration to use.
     * @return the configured auto credentials.
     */
    public static Collection<IAutoCredentials> GetAutoCredentials(Map storm_conf) {
        try {
            Set<IAutoCredentials> autos = new HashSet<IAutoCredentials>();
            Collection<String> clazzes = (Collection<String>)storm_conf.get(Config.TOPOLOGY_AUTO_CREDENTIALS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    IAutoCredentials a = (IAutoCredentials)Class.forName(clazz).newInstance();
                    a.prepare(storm_conf);
                    autos.add(a);
                }
            }
            LOG.info("Got AutoCreds "+autos);
            return autos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Populate a subject from credentials using the IAutoCredentials.
     * @param subject the subject to populate or null if a new Subject should be created.
     * @param autos the IAutoCredentials to call to populate the subject.
     * @param credentials the credentials to pull from
     * @return the populated subject.
     */
    public static Subject populateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String,String> credentials) {
        try {
            if (subject == null) {
                subject = new Subject();
            }
            for (IAutoCredentials autoCred : autos) {
                autoCred.populateSubject(subject, credentials);
            }
            return subject;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update a subject from credentials using the IAutoCredentials.
     * @param subject the subject to update
     * @param autos the IAutoCredentials to call to update the subject.
     * @param credentials the credentials to pull from
     */
    public static void updateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String,String> credentials) {
        if (subject == null) {
            throw new RuntimeException("The subject cannot be null when updating a subject with credentials");
        }

        try {
            for (IAutoCredentials autoCred : autos) {
                autoCred.updateSubject(subject, credentials);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct a transport plugin per storm configuration
     * @param conf storm configuration
     * @return
     */
    public static ITransportPlugin GetTransportPlugin(ThriftConnectionType type, Map storm_conf, Configuration login_conf) {
        ITransportPlugin  transportPlugin = null;
        try {
            String transport_plugin_klassName = type.getTransportPlugin(storm_conf);
            Class klass = Class.forName(transport_plugin_klassName);
            transportPlugin = (ITransportPlugin)klass.newInstance();
            transportPlugin.prepare(type, storm_conf, login_conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return transportPlugin;
    }

    private static IHttpCredentialsPlugin GetHttpCredentialsPlugin(Map conf,
            String klassName) {
        IHttpCredentialsPlugin plugin = null;
        try {
            Class klass = Class.forName(klassName);
            plugin = (IHttpCredentialsPlugin)klass.newInstance();
            plugin.prepare(conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return plugin;
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the UI
     * storm configuration
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin GetUiHttpCredentialsPlugin(Map conf) {
        String klassName = (String)conf.get(Config.UI_HTTP_CREDS_PLUGIN);
        return AuthUtils.GetHttpCredentialsPlugin(conf, klassName);
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the DRPC
     * storm configuration
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin GetDrpcHttpCredentialsPlugin(Map conf) {
        String klassName = (String)conf.get(Config.DRPC_HTTP_CREDS_PLUGIN);
        return AuthUtils.GetHttpCredentialsPlugin(conf, klassName);
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
