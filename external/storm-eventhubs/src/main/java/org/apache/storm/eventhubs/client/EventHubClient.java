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

import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubClient {

  private static final String DefaultConsumerGroupName = "$default";
  private static final Logger logger = LoggerFactory.getLogger(EventHubClient.class);
  private static final long ConnectionSyncTimeout = 60000L;

  private final String connectionString;
  private final String entityPath;
  private final Connection connection;

  private EventHubClient(String connectionString, String entityPath) throws EventHubException {
    this.connectionString = connectionString;
    this.entityPath = entityPath;
    this.connection = this.createConnection();
  }

  /**
   * creates a new instance of EventHubClient using the supplied connection string and entity path.
   *
   * @param connectionString connection string to the namespace of event hubs. connection string format:
   * amqps://{userId}:{password}@{namespaceName}.servicebus.windows.net
   * @param entityPath the name of event hub entity.
   *
   * @return EventHubClient
   * @throws org.apache.storm.eventhubs.client.EventHubException
   */
  public static EventHubClient create(String connectionString, String entityPath) throws EventHubException {
    return new EventHubClient(connectionString, entityPath);
  }

  public EventHubSender createPartitionSender(String partitionId) throws Exception {
    return new EventHubSender(this.connection.createSession(), this.entityPath, partitionId);
  }

  public EventHubConsumerGroup getDefaultConsumerGroup() {
    return new EventHubConsumerGroup(this.connection, this.entityPath, DefaultConsumerGroupName);
  }

  public void close() {
    try {
      this.connection.close();
    } catch (ConnectionErrorException e) {
      logger.error(e.toString());
    }
  }

  private Connection createConnection() throws EventHubException {
    ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(this.connectionString);
    Connection clientConnection;

    try {
      clientConnection = new Connection(
        connectionStringBuilder.getHost(),
        connectionStringBuilder.getPort(),
        connectionStringBuilder.getUserName(),
        connectionStringBuilder.getPassword(),
        connectionStringBuilder.getHost(),
        connectionStringBuilder.getSsl());
    } catch (ConnectionException e) {
      logger.error(e.toString());
      throw new EventHubException(e);
    }
    clientConnection.getEndpoint().setSyncTimeout(ConnectionSyncTimeout);
    SelectorFilterWriter.register(clientConnection.getEndpoint().getDescribedTypeRegistry());
    return clientConnection;
  }
}
