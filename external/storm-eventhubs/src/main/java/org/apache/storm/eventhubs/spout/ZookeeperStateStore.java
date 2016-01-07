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
package org.apache.storm.eventhubs.spout;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperStateStore implements IStateStore {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(ZookeeperStateStore.class);

  private final String zookeeperConnectionString;
  private final CuratorFramework curatorFramework;
  
  public ZookeeperStateStore(String zookeeperConnectionString) {
    this(zookeeperConnectionString, 3, 100);
  }

  public ZookeeperStateStore(String connectionString, int retries, int retryInterval) {
    if (connectionString == null) {
      zookeeperConnectionString = "localhost:2181";
    } else {
      zookeeperConnectionString = connectionString;
    }

    RetryPolicy retryPolicy = new RetryNTimes(retries, retryInterval);
    curatorFramework = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
  }

  @Override
  public void open() {
    curatorFramework.start();
  }

  @Override
  public void close() {
    curatorFramework.close();
  }

  @Override
  public void saveData(String statePath, String data) {
    data = data == null ? "" : data;
    byte[] bytes = data.getBytes();

    try {
      if (curatorFramework.checkExists().forPath(statePath) == null) {
        curatorFramework.create().creatingParentsIfNeeded().forPath(statePath, bytes);
      } else {
        curatorFramework.setData().forPath(statePath, bytes);
      }

      logger.info(String.format("data was saved. path: %s, data: %s.", statePath, data));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String readData(String statePath) {
    try {
      if (curatorFramework.checkExists().forPath(statePath) == null) {
        // do we want to throw an exception if path doesn't exist??
        return null;
      } else {
        byte[] bytes = curatorFramework.getData().forPath(statePath);
        String data = new String(bytes);

        logger.info(String.format("data was retrieved. path: %s, data: %s.", statePath, data));

        return data;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
