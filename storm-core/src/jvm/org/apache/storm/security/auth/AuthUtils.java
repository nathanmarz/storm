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
package org.apache.storm.security.auth;

import javax.security.auth.kerberos.KerberosTicket;
import org.apache.storm.Config;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.URIParameter;
import java.security.MessageDigest;

import org.apache.storm.security.INimbusCredentialPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.lang.StringBuilder;

public class AuthUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);
    public static final String LOGIN_CONTEXT_SERVER = "StormServer";
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient";
    public static final String LOGIN_CONTEXT_PACEMAKER_DIGEST = "PacemakerDigest";
    public static final String LOGIN_CONTEXT_PACEMAKER_SERVER = "PacemakerServer";
    public static final String LOGIN_CONTEXT_PACEMAKER_CLIENT = "PacemakerClient";
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
     * Pull a set of keys out of a Configuration.
     * @param conf The config to pull the key/value pairs out of.
     * @param conf_entry The app configuration entry name to get stuff from.
     * @return Return a map of the configs in conf.
     */
    public static SortedMap<String, ?> PullConfig(Configuration conf,
                                            String conf_entry) throws IOException {
        if(conf == null) {
            return null;
        }
        AppConfigurationEntry configurationEntries[] = conf.getAppConfigurationEntry(conf_entry);
        if(configurationEntries == null) {
            String errorMessage = "Could not find a '" + conf_entry
                + "' entry in this configuration: Client cannot start.";
            throw new IOException(errorMessage);
        }

        TreeMap<String, Object> results = new TreeMap<>();

        for(AppConfigurationEntry entry: configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            for(String key : options.keySet()) {
                results.put(key, options.get(key));
            }
        }
        return results;
    }

    /**
     * Construct a principal to local plugin
     * @param storm_conf storm configuration
     * @return the plugin
     */
    public static IPrincipalToLocal GetPrincipalToLocalPlugin(Map storm_conf) {
        IPrincipalToLocal ptol;
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
     * @param storm_conf storm configuration
     * @return the plugin
     */
    public static IGroupMappingServiceProvider GetGroupMappingServiceProviderPlugin(Map storm_conf) {
        IGroupMappingServiceProvider gmsp;
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
     * Get all of the configured Credential Renewer Plugins.
     * @param conf the storm configuration to use.
     * @return the configured credential renewers.
     */
    public static Collection<ICredentialsRenewer> GetCredentialRenewers(Map conf) {
        try {
            Set<ICredentialsRenewer> ret = new HashSet<>();
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
            Set<INimbusCredentialPlugin> ret = new HashSet<>();
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
            Set<IAutoCredentials> autos = new HashSet<>();
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
     */
    public static ITransportPlugin GetTransportPlugin(ThriftConnectionType type, Map storm_conf, Configuration login_conf) {
        ITransportPlugin  transportPlugin;
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
        IHttpCredentialsPlugin plugin;
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

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public static String makeDigestPayload(Configuration login_config, String config_section) {
        String username = null;
        String password = null;
        try {
            Map<String, ?> results = AuthUtils.PullConfig(login_config, config_section);
            username = (String)results.get(USERNAME);
            password = (String)results.get(PASSWORD);
        } catch (Exception e) {
            LOG.error("Failed to pull username/password out of jaas conf", e);
        }

        if(username == null || password == null) {
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] output = digest.digest((username + ":" + password).getBytes());

            StringBuilder builder = new StringBuilder();
            for(byte b : output) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            LOG.error("Cant run SHA-512 digest. Algorithm not available.", e);
            throw new RuntimeException(e);
        }
    }

    public static byte[] serializeKerberosTicket(KerberosTicket tgt) throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bao);
        out.writeObject(tgt);
        out.flush();
        out.close();
        return bao.toByteArray();
    }

    public static KerberosTicket deserializeKerberosTicket(byte[] tgtBytes) {
        KerberosTicket ret;
        try {

            ByteArrayInputStream bin = new ByteArrayInputStream(tgtBytes);
            ObjectInputStream in = new ObjectInputStream(bin);
            ret = (KerberosTicket)in.readObject();
            in.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static KerberosTicket cloneKerberosTicket(KerberosTicket kerberosTicket) {
        if(kerberosTicket != null) {
            try {
                return (deserializeKerberosTicket(serializeKerberosTicket(kerberosTicket)));
            } catch (Exception e) {
                throw new RuntimeException("Failed to clone KerberosTicket TGT!!", e);
            }
        }
        return null;
    }
}
