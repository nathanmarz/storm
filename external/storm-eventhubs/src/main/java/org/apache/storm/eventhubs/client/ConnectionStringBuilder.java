/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.client;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLStreamHandler;

public class ConnectionStringBuilder {

  private final String connectionString;

  private String host;
  private int port;
  private String userName;
  private String password;
  private boolean ssl;

  // amqps://[username]:[password]@[namespace].servicebus.windows.net/
  public ConnectionStringBuilder(String connectionString) throws EventHubException {
    this.connectionString = connectionString;
    this.initialize();
  }

  public String getHost() {
    return this.host;
  }

  public void setHost(String value) {
    this.host = value;
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(int value) {
    this.port = value;
  }

  public String getUserName() {
    return this.userName;
  }

  public void setUserName(String value) {
    this.userName = value;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String value) {
    this.password = value;
  }

  public boolean getSsl() {
    return this.ssl;
  }

  public void setSsl(boolean value) {
    this.ssl = value;
  }

  private void initialize() throws EventHubException {

    URL url;
    try {
      url = new URL(null, this.connectionString, new NullURLStreamHandler());
    } catch (MalformedURLException e) {
      throw new EventHubException("connectionString is not valid.", e);
    }

    String protocol = url.getProtocol();
    this.ssl = protocol.equalsIgnoreCase(Constants.SslScheme);
    this.host = url.getHost();
    this.port = url.getPort();

    if (this.port == -1) {
      this.port = this.ssl ? Constants.DefaultSslPort : Constants.DefaultPort;
    }

    String userInfo = url.getUserInfo();
    if (userInfo != null) {
      String[] credentials = userInfo.split(":", 2);
      this.userName = URLDecoder.decode(credentials[0]);
      this.password = URLDecoder.decode(credentials[1]);
    }
  }

  class NullURLStreamHandler extends URLStreamHandler {

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
