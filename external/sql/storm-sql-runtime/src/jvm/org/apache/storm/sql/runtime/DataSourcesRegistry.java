/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * <p>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.storm.sql.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class DataSourcesRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(
      DataSourcesRegistry.class);
  private static final Map<String, DataSourcesProvider> providers;

  static {
    providers = new HashMap<>();
    ServiceLoader<DataSourcesProvider> loader = ServiceLoader.load(
        DataSourcesProvider.class);
    for (DataSourcesProvider p : loader) {
      LOG.info("Registering scheme {} with {}", p.scheme(), p);
      providers.put(p.scheme(), p);
    }
  }

  private DataSourcesRegistry() {
  }

  public static DataSource construct(
      URI uri, String inputFormatClass, String outputFormatClass,
      List<FieldInfo> fields) {
    DataSourcesProvider provider = providers.get(uri.getScheme());
    if (provider == null) {
      return null;
    }

    return provider.construct(uri, inputFormatClass, outputFormatClass, fields);
  }

  public static ISqlTridentDataSource constructTridentDataSource(
      URI uri, String inputFormatClass, String outputFormatClass,
      String properties, List<FieldInfo> fields) {
    DataSourcesProvider provider = providers.get(uri.getScheme());
    if (provider == null) {
      return null;
    }

    return provider.constructTrident(uri, inputFormatClass, outputFormatClass, properties, fields);
  }

  /**
   * Allow unit tests to inject data sources.
   */
  public static Map<String, DataSourcesProvider> providerMap() {
    return providers;
  }
}
