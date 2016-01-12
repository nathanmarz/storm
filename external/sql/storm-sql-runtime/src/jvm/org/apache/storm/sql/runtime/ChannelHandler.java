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

/**
 * DataListener provides an event-driven interface for the user to process
 * series of events.
 */
public interface ChannelHandler {
  void dataReceived(ChannelContext ctx, Values data);

  /**
   * The producer of the data has indicated that the channel is no longer
   * active.
   * @param ctx
   */
  void channelInactive(ChannelContext ctx);

  void exceptionCaught(Throwable cause);
}
