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
package org.apache.storm.sql;

import backtype.storm.tuple.Values;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
  public static class MockDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockDataSource() {
      for (int i = 0; i < 5; ++i) {
        RECORDS.add(new Values(i));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.fireChannelInactive();
    }
  }

  public static class CollectDataChannelHandler implements ChannelHandler {
    private final List<Values> values;

    public CollectDataChannelHandler(List<Values> values) {
      this.values = values;
    }

    @Override
    public void dataReceived(ChannelContext ctx, Values data) {
      values.add(data);
    }

    @Override
    public void channelInactive(ChannelContext ctx) {}

    @Override
    public void exceptionCaught(Throwable cause) {
      throw new RuntimeException(cause);
    }
  }
}
