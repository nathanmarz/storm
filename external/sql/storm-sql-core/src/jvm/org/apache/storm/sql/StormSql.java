/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql;

import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.sql.runtime.ChannelHandler;

import java.util.Map;

/**
 * The StormSql class provides standalone, interactive interfaces to execute
 * SQL statements over streaming data.
 * <p>
 * The StormSql class is stateless. The user needs to submit the data
 * definition language (DDL) statements and the query statements in the same
 * batch.
 */
public abstract class StormSql {
  /**
   * Execute the SQL statements in stand-alone mode. The user can retrieve the result by passing in an instance
   * of {@see ChannelHandler}.
   */
  public abstract void execute(Iterable<String> statements,
                               ChannelHandler handler) throws Exception;

  /**
   * Submit the SQL statements to Nimbus and run it as a topology.
   */
  public abstract void submit(
      String name, Iterable<String> statements, Map<String, ?> stormConf, SubmitOptions opts,
      StormSubmitter.ProgressListener progressListener, String asUser)
      throws Exception;

  public static StormSql construct() {
    return new StormSqlImpl();
  }
}

