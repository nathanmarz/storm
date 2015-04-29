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

import org.apache.qpid.amqp_1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.amqp_1_0.codec.ValueWriter;
import org.apache.qpid.amqp_1_0.type.UnsignedLong;

public class SelectorFilterWriter extends
  AbstractDescribedTypeWriter<SelectorFilter> {

  private static final ValueWriter.Factory<SelectorFilter> FACTORY = new ValueWriter.Factory<SelectorFilter>() {

    @Override
    public ValueWriter<SelectorFilter> newInstance(ValueWriter.Registry registry) {
      return new SelectorFilterWriter(registry);
    }
  };

  private SelectorFilter value;

  public SelectorFilterWriter(final ValueWriter.Registry registry) {
    super(registry);
  }

  public static void register(ValueWriter.Registry registry) {
    registry.register(SelectorFilter.class, FACTORY);
  }

  @Override
  protected void onSetValue(final SelectorFilter value) {
    this.value = value;
  }

  @Override
  protected void clear() {
    value = null;
  }

  @Override
  protected Object getDescriptor() {
    return UnsignedLong.valueOf(0x00000137000000AL);
  }

  @Override
  protected ValueWriter<String> createDescribedWriter() {
    return getRegistry().getValueWriter(value.getValue());
  }
}
