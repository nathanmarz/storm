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

public class Channels {
  private static final ChannelContext VOID_CTX = new ChannelContext() {
    @Override
    public void emit(Values data) {}

    @Override
    public void fireChannelInactive() {}
  };

  private static class ChannelContextAdapter implements ChannelContext {
    private final ChannelHandler handler;
    private final ChannelContext next;

    public ChannelContextAdapter(
        ChannelContext next, ChannelHandler handler) {
      this.handler = handler;
      this.next = next;
    }

    @Override
    public void emit(Values data) {
      handler.dataReceived(next, data);
    }

    @Override
    public void fireChannelInactive() {
      handler.channelInactive(next);
    }
  }

  private static class ForwardingChannelContext implements ChannelContext {
    private final ChannelContext next;

    public ForwardingChannelContext(ChannelContext next) {
      this.next = next;
    }

    @Override
    public void emit(Values data) {
      next.emit(data);
    }

    @Override
    public void fireChannelInactive() {
      next.fireChannelInactive();
    }
  }

  public static ChannelContext chain(
      ChannelContext next, ChannelHandler handler) {
    return new ChannelContextAdapter(next, handler);
  }

  public static ChannelContext voidContext() {
    return VOID_CTX;
  }
}
