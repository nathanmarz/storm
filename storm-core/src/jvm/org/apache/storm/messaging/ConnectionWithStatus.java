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
package org.apache.storm.messaging;

public abstract class ConnectionWithStatus implements IConnection {

  public static enum Status {

    /**
     * we are establishing a active connection with target host. The new data
     * sending request can be buffered for future sending, or dropped(cases like
     * there is no enough memory). It varies with difference IConnection
     * implementations.
     */
    Connecting,

    /**
     * We have a alive connection channel, which can be used to transfer data.
     */
    Ready,

    /**
     * The connection channel is closed or being closed. We don't accept further
     * data sending or receiving. All data sending request will be dropped.
     */
    Closed
  }

    /**
   * whether this connection is available to transfer data
   */
  public abstract Status status();

}
