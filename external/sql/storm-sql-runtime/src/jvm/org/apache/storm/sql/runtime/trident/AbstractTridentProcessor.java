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

package org.apache.storm.sql.runtime.trident;

import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;

import java.util.Map;

public abstract class AbstractTridentProcessor {
  protected Stream outputStream;
  /**
   * @return the output stream of the SQL
   */
  public Stream outputStream() {
    return outputStream;
  }

  /**
   * Construct the trident topology based on the SQL.
   * @param sources the data sources.
   */
  public abstract TridentTopology build(Map<String, ISqlTridentDataSource> sources);
}
