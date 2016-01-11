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

import org.apache.storm.tuple.Values;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;

import java.util.Map;

/**
 * Subclass of AbstractTupleProcessor provides a series of tuple. It
 * takes a series of iterators of {@link Values} and produces a stream of
 * tuple.
 *
 * The subclass implements the {@see next()} method to provide
 * the output of the stream. It can choose to return null in {@see next()} to
 * indicate that this particular iteration is a no-op. SQL processors depend
 * on this semantic to implement filtering and nullable records.
 */
public abstract class AbstractValuesProcessor {

  /**
   * Initialize the data sources.
   *
   * @param data a map from the table name to the iterators of the values.
   *
   */
  public abstract void initialize(Map<String, DataSource> data, ChannelHandler
      result);
}
