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

public class Constants {

  public static final String DefaultStartingOffset = "-1";
  public static final String SelectorFilterName = "apache.org:selector-filter:string";
  public static final String OffsetFilterFormatString = "amqp.annotation.x-opt-offset > '%s'";
  public static final String EnqueueTimeFilterFormatString = "amqp.annotation.x-opt-enqueuedtimeutc > %d";
  public static final String ConsumerAddressFormatString = "%s/ConsumerGroups/%s/Partitions/%s";
  public static final String DestinationAddressFormatString = "%s/Partitions/%s";

  public static final String SslScheme = "amqps";
  public static final int DefaultPort = 5672;
  public static final int DefaultSslPort = 5671;
}
