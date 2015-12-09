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

import java.net.URI;
import java.util.List;

public interface DataSourcesProvider {
  /**
   * @return the scheme of the data source
   */
  String scheme();

  /**
   * Construct a new data source.
   * @param uri The URI that specifies the data source. The format of the URI
   *            is fully customizable.
   * @param inputFormatClass the name of the class that deserializes data.
   *                         It is null when unspecified.
   * @param outputFormatClass the name of the class that serializes data. It
   *                          is null when unspecified.
   * @param fields The name of the fields and the schema of the table.
   */
  DataSource construct(
      URI uri, String inputFormatClass, String outputFormatClass,
      List<FieldInfo> fields);

  ISqlTridentDataSource constructTrident(
      URI uri, String inputFormatClass, String outputFormatClass,
      String properties, List<FieldInfo> fields);
}
